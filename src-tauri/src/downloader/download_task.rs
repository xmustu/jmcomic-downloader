use std::{
    collections::BTreeMap,
    fs::File,
    io::Write,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    time::Duration,
};

use eyre::{eyre, OptionExt, WrapErr};
use tauri::AppHandle;
use tauri_specta::Event;
use tokio::{
    sync::{mpsc, watch, SemaphorePermit},
    task::JoinSet,
    time::sleep,
};
use tracing::{instrument, Instrument};
use zip::{write::SimpleFileOptions, ZipWriter};

use crate::{
    downloader::{
        download_img_task::{calculate_block_num, DownloadImgOutput, DownloadImgTask},
        download_task_state::DownloadTaskState,
    },
    events::DownloadEvent,
    extensions::{AppHandleExt, EyreReportToMessage},
    jm_client::IMAGE_DOMAIN,
    types::{ChapterInfo, Comic, ComicInfo},
};

pub struct DownloadTask {
    pub app: AppHandle,
    pub comic: Arc<Comic>,
    pub chapter_info: Arc<ChapterInfo>,
    pub state_sender: watch::Sender<DownloadTaskState>,
    pub delete_sender: watch::Sender<()>,
    pub downloaded_img_count: Arc<AtomicU32>,
    pub total_img_count: Arc<AtomicU32>,
    download_chapters_as_cbz: bool,
}

struct TempCbzGuard(PathBuf);

impl Drop for TempCbzGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

impl DownloadTask {
    #[instrument(
        level = "error",
        skip_all,
        fields(
            comic_id = comic.id,
            comic_title = comic.name,
            chapter_id = chapter_id
        )
    )]
    pub fn new(app: AppHandle, mut comic: Comic, chapter_id: i64) -> eyre::Result<Arc<Self>> {
        comic
            .ensure_download_dir_fields(&app)
            .wrap_err("更新下载目录字段失败")?;

        let chapter_info = comic
            .chapter_infos
            .iter()
            .find(|chapter| chapter.chapter_id == chapter_id)
            .cloned()
            .ok_or_eyre(format!("未找到章节ID为`{chapter_id}`的章节信息"))?;

        let download_chapters_as_cbz = app.get_config().read().download_chapters_as_cbz;
        let (state_sender, _) = watch::channel(DownloadTaskState::Pending);
        let (delete_sender, _) = watch::channel(());

        let task = Arc::new(Self {
            app,
            comic: Arc::new(comic),
            chapter_info: Arc::new(chapter_info),
            state_sender,
            delete_sender,
            downloaded_img_count: Arc::new(AtomicU32::new(0)),
            total_img_count: Arc::new(AtomicU32::new(0)),
            download_chapters_as_cbz,
        });

        tauri::async_runtime::spawn(task.clone().process());

        Ok(task)
    }

    #[instrument(
        level = "error",
        skip_all,
        fields(
            comic_id = self.comic.id,
            comic_title = self.comic.name,
            chapter_id = self.chapter_info.chapter_id,
            chapter_title = self.chapter_info.chapter_title,
            order = self.chapter_info.order
        )
    )]
    async fn process(self: Arc<Self>) {
        self.emit_download_task_create_event();

        let mut state_receiver = self.state_sender.subscribe();
        state_receiver.mark_changed();

        let mut delete_receiver = self.delete_sender.subscribe();

        let mut permit = None;
        let mut download_task_option = None;

        loop {
            let state = *state_receiver.borrow();
            let state_is_downloading = state == DownloadTaskState::Downloading;
            let state_is_pending = state == DownloadTaskState::Pending;

            let download_task = async {
                download_task_option
                    .get_or_insert_with(|| Box::pin(self.download_chapter()))
                    .await;
            };

            tokio::select! {
                () = download_task, if state_is_downloading && permit.is_some() => {
                    download_task_option = None;
                    if let Some(permit) = permit.take() {
                        drop(permit);
                    }
                }

                () = self.acquire_chapter_permit(&mut permit), if state_is_pending => {}

                _ = state_receiver.changed() => {
                    self.handle_state_change(&mut permit, &mut state_receiver).await;
                }

                _ = delete_receiver.changed() => {
                    self.handle_delete_receiver_change(&mut permit).await;
                    return;
                }
            }
        }
    }

    #[instrument(level = "error", skip_all)]
    async fn download_chapter(self: &Arc<Self>) {
        let chapter_id = self.chapter_info.chapter_id;

        if let Err(err) = self.comic.save_comic_metadata() {
            let err_title = "保存元数据失败";
            let message = err.to_message();
            tracing::error!(err_title, message);

            self.set_state(DownloadTaskState::Failed);
            self.emit_download_task_update_event();

            return;
        }

        let should_download_cover = self.app.get_config().read().should_download_cover;
        if should_download_cover {
            if let Err(err) = self.download_cover().await {
                let err_title = "下载封面失败";
                let message = err.to_message();
                tracing::error!(err_title, message);

                self.set_state(DownloadTaskState::Failed);
                self.emit_download_task_update_event();

                return;
            }
        }

        let Some(urls_with_block_num) = self.get_urls_with_block_num(chapter_id).await else {
            return;
        };

        #[allow(clippy::cast_possible_truncation)]
        self.total_img_count
            .fetch_add(urls_with_block_num.len() as u32, Ordering::Relaxed);

        let cbz_mode = self.download_chapters_as_cbz;
        let fallback_urls = urls_with_block_num.clone();
        let mut temp_download_dir = None;
        let mut temp_cbz_path = None;
        let mut final_cbz_path = None;
        let mut cbz_sender = None;
        let mut cbz_writer = None;
        let mut _temp_cbz_guard = None;

        if cbz_mode {
            match self.start_cbz_writer(urls_with_block_num.len()) {
                Ok((temporary, destination, sender, writer)) => {
                    temp_cbz_path = Some(temporary);
                    _temp_cbz_guard = Some(TempCbzGuard(
                        temp_cbz_path.as_ref().expect("temp CBZ path set").clone(),
                    ));
                    final_cbz_path = Some(destination);
                    cbz_sender = Some(sender);
                    cbz_writer = Some(writer);
                }
                Err(err) => {
                    self.fallback_to_image_directory(&fallback_urls, err).await;
                    return;
                }
            }
        } else {
            let Some(directory) = self.create_temp_download_dir() else {
                return;
            };
            self.clean_temp_download_dir(&directory);
            temp_download_dir = Some(directory);
        }

        let batch_size = if cbz_mode {
            self.app.get_config().read().img_concurrency.max(1)
        } else {
            urls_with_block_num.len().max(1)
        };
        for (batch_index, batch) in urls_with_block_num.chunks(batch_size).enumerate() {
            let mut join_set = JoinSet::new();
            for (index_in_batch, (url, block_num)) in batch.iter().enumerate() {
                let image_index = batch_index * batch_size + index_in_batch;
                let output = if let Some(sender) = &cbz_sender {
                    DownloadImgOutput::Cbz(sender.clone())
                } else {
                    DownloadImgOutput::Directory(
                        temp_download_dir
                            .as_ref()
                            .expect("image directory was created")
                            .clone(),
                    )
                };
                let download_img_task = DownloadImgTask::new(
                    self.clone(),
                    url.clone(),
                    image_index,
                    output,
                    *block_num,
                );
                join_set.spawn(download_img_task.process().in_current_span());
            }
            join_set.join_all().await;
            let expected_count = (batch_index * batch_size + batch.len()) as u32;
            if self.downloaded_img_count.load(Ordering::Relaxed) < expected_count {
                break;
            }
        }
        drop(cbz_sender);

        let cbz_result = if let Some(writer) = cbz_writer {
            Some(match writer.await {
                Ok(result) => result,
                Err(err) => Err(eyre!("CBZ写入任务异常退出: {err}")),
            })
        } else {
            None
        };

        tracing::trace!("所有图片下载任务完成");

        let downloaded_img_count = self.downloaded_img_count.load(Ordering::Relaxed);
        let total_img_count = self.total_img_count.load(Ordering::Relaxed);
        if downloaded_img_count != total_img_count {
            let err_title = "下载不完整";
            let message =
                eyre!("总共有`{total_img_count}`张图片，但只下载了`{downloaded_img_count}`张")
                    .to_message();
            tracing::error!(err_title, message);

            if let Some(path) = &temp_cbz_path {
                let _ = std::fs::remove_file(path);
            }

            self.set_state(DownloadTaskState::Failed);
            self.emit_download_task_update_event();

            return;
        }

        if let Some(Err(err)) = cbz_result {
            if let Some(path) = &temp_cbz_path {
                let _ = std::fs::remove_file(path);
            }
            self.fallback_to_image_directory(&fallback_urls, err).await;
            return;
        }

        if cbz_mode {
            let temporary = temp_cbz_path.as_ref().expect("CBZ temp path must exist");
            let destination = final_cbz_path.as_ref().expect("CBZ destination must exist");
            if let Err(err) = self.finalize_cbz(temporary, destination) {
                let _ = std::fs::remove_file(temporary);
                self.fallback_to_image_directory(&fallback_urls, err).await;
                return;
            }
        } else if let Some(directory) = &temp_download_dir {
            if let Err(err) = self.rename_temp_download_dir(directory) {
                let err_title = "保存下载目录失败";
                let message = err.to_message();
                tracing::error!(err_title, message);

                self.set_state(DownloadTaskState::Failed);
                self.emit_download_task_update_event();

                return;
            }
        }

        if let Err(err) = self.chapter_info.save_chapter_metadata() {
            let err_title = "保存章节元数据失败";
            let message = err.to_message();
            tracing::error!(err_title, message);
        }

        self.sleep_between_chapter().await;
        tracing::info!("章节下载成功");

        self.set_state(DownloadTaskState::Completed);
        self.emit_download_task_update_event();
    }

    fn start_cbz_writer(
        &self,
        expected_pages: usize,
    ) -> eyre::Result<(
        PathBuf,
        PathBuf,
        mpsc::Sender<(usize, String, Vec<u8>)>,
        tokio::task::JoinHandle<eyre::Result<()>>,
    )> {
        let chapter_dir = self
            .chapter_info
            .chapter_download_dir
            .as_ref()
            .ok_or_eyre("`chapter_download_dir`字段为`None`")?;
        let chapter_name = self.chapter_info.get_chapter_download_dir_name()?;
        let parent = chapter_dir
            .parent()
            .ok_or_eyre("章节下载目录没有父目录")?;
        std::fs::create_dir_all(parent)
            .wrap_err(format!("创建目录`{}`失败", parent.display()))?;

        let destination = parent.join(format!("{chapter_name}.cbz"));
        let temporary = parent.join(format!(".下载中-{chapter_name}.cbz"));
        if temporary.exists() {
            std::fs::remove_file(&temporary)
                .wrap_err(format!("删除旧临时CBZ`{}`失败", temporary.display()))?;
        }

        let comic_info = ComicInfo::from(&self.comic, &self.chapter_info);
        let comic_info_xml = yaserde::ser::to_string_with_config(
            &comic_info,
            &yaserde::ser::Config {
                perform_indent: true,
                ..Default::default()
            },
        )
        .map_err(|err| eyre!("序列化`ComicInfo.xml`失败: {err}"))?;

        let queue_size = self.app.get_config().read().img_concurrency.max(1);
        let (sender, receiver) = mpsc::channel(queue_size);
        let writer_path = temporary.clone();
        let writer = tokio::task::spawn_blocking(move || {
            write_cbz_archive(writer_path, expected_pages, comic_info_xml, receiver)
        });

        Ok((temporary, destination, sender, writer))
    }

    fn finalize_cbz(&self, temporary: &Path, destination: &Path) -> eyre::Result<()> {
        let chapter_dir = self
            .chapter_info
            .chapter_download_dir
            .as_ref()
            .ok_or_eyre("`chapter_download_dir`字段为`None`")?;

        if chapter_dir.exists() {
            std::fs::remove_dir_all(chapter_dir)
                .wrap_err(format!("删除章节图片目录`{}`失败", chapter_dir.display()))?;
        }
        if destination.exists() {
            std::fs::remove_file(destination)
                .wrap_err(format!("删除旧CBZ`{}`失败", destination.display()))?;
        }
        std::fs::rename(temporary, destination).wrap_err(format!(
            "将临时CBZ`{}`保存为`{}`失败",
            temporary.display(),
            destination.display()
        ))?;

        Ok(())
    }

    async fn fallback_to_image_directory(
        self: &Arc<Self>,
        urls_with_block_num: &[(String, u32)],
        cbz_error: eyre::Report,
    ) {
        let message = cbz_error.to_message();
        tracing::error!(err_title = "生成CBZ失败，回退为图片目录", message);

        if urls_with_block_num.is_empty() {
            tracing::error!("章节没有图片，无法生成CBZ或回退图片目录");
            self.set_state(DownloadTaskState::Failed);
            self.emit_download_task_update_event();
            return;
        }

        self.downloaded_img_count.store(0, Ordering::Relaxed);
        self.emit_download_task_update_event();

        let Some(temp_download_dir) = self.create_temp_download_dir() else {
            return;
        };
        self.clean_temp_download_dir(&temp_download_dir);

        let batch_size = self.app.get_config().read().img_concurrency.max(1);
        for (batch_index, batch) in urls_with_block_num.chunks(batch_size).enumerate() {
            let mut join_set = JoinSet::new();
            for (index_in_batch, (url, block_num)) in batch.iter().enumerate() {
                let image_index = batch_index * batch_size + index_in_batch;
                let download_img_task = DownloadImgTask::new(
                    self.clone(),
                    url.clone(),
                    image_index,
                    DownloadImgOutput::Directory(temp_download_dir.clone()),
                    *block_num,
                );
                join_set.spawn(download_img_task.process().in_current_span());
            }
            join_set.join_all().await;
            let expected_count = (batch_index * batch_size + batch.len()) as u32;
            if self.downloaded_img_count.load(Ordering::Relaxed) < expected_count {
                break;
            }
        }

        let downloaded_img_count = self.downloaded_img_count.load(Ordering::Relaxed);
        let total_img_count = self.total_img_count.load(Ordering::Relaxed);
        let fallback_succeeded = if downloaded_img_count == total_img_count {
            match self.rename_temp_download_dir(&temp_download_dir) {
                Ok(()) => true,
                Err(err) => {
                    let message = err.to_message();
                    tracing::error!(err_title = "回退图片目录保存失败", message);
                    false
                }
            }
        } else {
            false
        };

        if fallback_succeeded {
            if let Err(err) = self.chapter_info.save_chapter_metadata() {
                let message = err.to_message();
                tracing::error!(err_title = "回退后保存章节元数据失败", message);
            }
            tracing::error!("CBZ未生成；章节图片已回退保存为普通文件");
        } else {
            let _ = std::fs::remove_dir_all(&temp_download_dir);
            tracing::error!(
                "CBZ未生成，且无法完整回退为图片目录 ({downloaded_img_count}/{total_img_count})"
            );
        }

        self.set_state(DownloadTaskState::Failed);
        self.emit_download_task_update_event();
    }

    #[instrument(level = "error", skip_all)]
    async fn download_cover(&self) -> eyre::Result<()> {
        let cover_path = self.comic.get_cover_path().wrap_err("获取封面路径失败")?;

        let comic_id = self.comic.id;
        let url = format!("https://cdn-msp3.18comic.vip/media/albums/{comic_id}.jpg");

        let (img_data, _format) = self
            .app
            .get_jm_client()
            .get_img_data_and_format(&url)
            .await
            .wrap_err(format!("下载图片`{url}`失败"))?;

        std::fs::write(&cover_path, img_data)
            .wrap_err(format!("保存图片`{}`失败", cover_path.display()))?;

        Ok(())
    }

    #[instrument(level = "error", skip_all)]
    fn create_temp_download_dir(&self) -> Option<PathBuf> {
        let temp_download_dir = match self.chapter_info.get_temp_download_dir() {
            Ok(temp_download_dir) => temp_download_dir,
            Err(err) => {
                let err_title = "获取临时下载目录失败";
                let message = err.to_message();
                tracing::error!(err_title, message);

                self.set_state(DownloadTaskState::Failed);
                self.emit_download_task_update_event();

                return None;
            }
        };

        if let Err(err) = std::fs::create_dir_all(&temp_download_dir).map_err(eyre::Report::from) {
            let err_title = "创建临时下载目录失败";
            let message = err.to_message();
            tracing::error!(err_title, message);

            self.set_state(DownloadTaskState::Failed);
            self.emit_download_task_update_event();

            return None;
        }

        tracing::trace!("创建临时下载目录成功");

        Some(temp_download_dir)
    }

    #[instrument(level = "error", skip_all, fields(temp_download_dir = %temp_download_dir.display()))]
    fn rename_temp_download_dir(&self, temp_download_dir: &Path) -> eyre::Result<()> {
        let chapter_download_dir = self
            .chapter_info
            .chapter_download_dir
            .as_ref()
            .ok_or_eyre("`chapter_download_dir`字段为`None`")?;

        if chapter_download_dir.exists() {
            std::fs::remove_dir_all(chapter_download_dir)
                .wrap_err(format!("删除 `{}` 失败", chapter_download_dir.display()))?;
        }

        std::fs::rename(temp_download_dir, chapter_download_dir).wrap_err(format!(
            "将 `{}` 重命名为 `{}` 失败",
            temp_download_dir.display(),
            chapter_download_dir.display()
        ))?;

        Ok(())
    }

    #[instrument(level = "error", skip_all)]
    async fn get_urls_with_block_num(&self, chapter_id: i64) -> Option<Vec<(String, u32)>> {
        let jm_client = self.app.get_jm_client();

        let res = tokio::try_join!(
            jm_client.get_scramble_id(chapter_id),
            jm_client.get_chapter(chapter_id)
        );

        let (scramble_id, chapter_resp_data) = match res {
            Ok(data) => data,
            Err(err) => {
                let err_title = "获取图片下载链接失败";
                let message = err.to_message();
                tracing::error!(err_title, message);

                self.set_state(DownloadTaskState::Failed);
                self.emit_download_task_update_event();

                return None;
            }
        };

        let urls_with_block_num: Vec<(String, u32)> = chapter_resp_data
            .images
            .into_iter()
            .filter_map(|filename| {
                let file_path = Path::new(&filename);
                let ext = file_path.extension()?.to_str()?.to_lowercase();
                let url = format!("https://{IMAGE_DOMAIN}/media/photos/{chapter_id}/{filename}");
                if ext == "gif" {
                    return Some((url, 0));
                } else if ext != "webp" {
                    return None;
                }

                let filename_without_ext = file_path.file_stem()?.to_str()?;
                let block_num = calculate_block_num(scramble_id, chapter_id, filename_without_ext);
                Some((url, block_num))
            })
            .collect();

        tracing::trace!("获取图片链接成功");

        Some(urls_with_block_num)
    }

    #[instrument(level = "error", skip_all, fields(temp_download_dir = %temp_download_dir.display()))]
    fn clean_temp_download_dir(&self, temp_download_dir: &Path) {
        let entries = match std::fs::read_dir(temp_download_dir).map_err(eyre::Report::from) {
            Ok(entries) => entries,
            Err(err) => {
                let err_title = "读取临时下载目录失败";
                let message = err.to_message();
                tracing::error!(err_title, message);
                return;
            }
        };

        let download_format = self.app.get_config().read().download_format;
        let extension = download_format.extension();
        for path in entries.filter_map(Result::ok).map(|entry| entry.path()) {
            let should_keep = path
                .extension()
                .and_then(|ext| ext.to_str())
                .is_some_and(|ext| ext == "gif" || ext == extension);
            if should_keep {
                continue;
            }

            if let Err(err) = std::fs::remove_file(&path).map_err(eyre::Report::from) {
                let err_title = "删除临时下载目录中的文件失败";
                let message = err.to_message();
                tracing::error!(err_title, message);
            }
        }

        tracing::trace!("清理临时下载目录成功");
    }

    #[instrument(level = "error", skip_all)]
    async fn acquire_chapter_permit<'a>(&'a self, permit: &mut Option<SemaphorePermit<'a>>) {
        tracing::debug!("章节开始排队");

        self.emit_download_task_update_event();

        *permit = match permit.take() {
            Some(permit) => Some(permit),
            None => match self
                .app
                .get_download_manager()
                .inner()
                .chapter_sem
                .acquire()
                .await
                .map_err(eyre::Report::from)
            {
                Ok(permit) => Some(permit),
                Err(err) => {
                    let err_title = "获取下载章节的permit失败";
                    let message = err.to_message();
                    tracing::error!(err_title, message);

                    self.set_state(DownloadTaskState::Failed);
                    self.emit_download_task_update_event();
                    return;
                }
            },
        };

        if *self.state_sender.borrow() != DownloadTaskState::Pending {
            return;
        }

        if let Err(err) = self
            .state_sender
            .send(DownloadTaskState::Downloading)
            .map_err(eyre::Report::from)
        {
            let err_title = "发送状态`Downloading`失败";
            let message = err.to_message();
            tracing::error!(err_title, message);
            self.set_state(DownloadTaskState::Failed);
        }
    }

    #[instrument(level = "error", skip_all)]
    async fn handle_state_change<'a>(
        &'a self,
        permit: &mut Option<SemaphorePermit<'a>>,
        state_receiver: &mut watch::Receiver<DownloadTaskState>,
    ) {
        self.emit_download_task_update_event();

        let state = *state_receiver.borrow();
        if state == DownloadTaskState::Paused {
            sleep(Duration::from_millis(100)).await;
            tracing::debug!("下载任务已暂停");
            if let Some(permit) = permit.take() {
                drop(permit);
            }
        } else if state == DownloadTaskState::Failed {
            sleep(Duration::from_millis(100)).await;
            if let Some(permit) = permit.take() {
                drop(permit);
            }
        }
    }

    #[instrument(level = "error", skip_all)]
    async fn handle_delete_receiver_change<'a>(&'a self, permit: &mut Option<SemaphorePermit<'a>>) {
        let chapter_id = self.chapter_info.chapter_id;

        let _ = DownloadEvent::TaskDelete { chapter_id }.emit(&self.app);

        if permit.is_some() {
            sleep(Duration::from_millis(100)).await;
        }

        tracing::debug!("下载任务已删除");
    }

    #[instrument(level = "error", skip_all)]
    async fn sleep_between_chapter(&self) {
        let mut remaining_sec = self.app.get_config().read().chapter_download_interval_sec;
        while remaining_sec > 0 {
            let _ = DownloadEvent::Sleeping {
                chapter_id: self.chapter_info.chapter_id,
                remaining_sec,
            }
            .emit(&self.app);
            sleep(Duration::from_secs(1)).await;
            remaining_sec -= 1;
        }
    }

    #[instrument(
        level = "error",
        skip_all,
        fields(
            comic_id = self.comic.id,
            comic_title = self.comic.name,
            chapter_id = self.chapter_info.chapter_id,
            chapter_title = self.chapter_info.chapter_title,
            order = self.chapter_info.order
        )
    )]
    pub fn set_state(&self, state: DownloadTaskState) {
        if let Err(err) = self.state_sender.send(state).map_err(eyre::Report::from) {
            let err_title = format!("发送状态`{state:?}`失败");
            let message = err.to_message();
            tracing::error!(err_title, message);
        }
    }

    pub fn emit_download_task_update_event(&self) {
        let is_downloaded = self
            .chapter_info
            .chapter_download_dir
            .as_ref()
            .is_some_and(|path| path.join("章节元数据.json").is_file());
        let _ = DownloadEvent::TaskUpdate {
            chapter_id: self.chapter_info.chapter_id,
            state: *self.state_sender.borrow(),
            downloaded_img_count: self.downloaded_img_count.load(Ordering::Relaxed),
            total_img_count: self.total_img_count.load(Ordering::Relaxed),
            is_downloaded,
        }
        .emit(&self.app);
    }

    fn emit_download_task_create_event(&self) {
        let _ = DownloadEvent::TaskCreate {
            state: *self.state_sender.borrow(),
            comic: Box::new(self.comic.as_ref().clone()),
            chapter_info: Box::new(self.chapter_info.as_ref().clone()),
            downloaded_img_count: self.downloaded_img_count.load(Ordering::Relaxed),
            total_img_count: self.total_img_count.load(Ordering::Relaxed),
        }
        .emit(&self.app);
    }
}

fn write_cbz_archive(
    path: PathBuf,
    expected_pages: usize,
    comic_info_xml: String,
    receiver: mpsc::Receiver<(usize, String, Vec<u8>)>,
) -> eyre::Result<()> {
    let result = write_cbz_archive_inner(
        &path,
        expected_pages,
        comic_info_xml,
        receiver,
    );
    if result.is_err() {
        let _ = std::fs::remove_file(path);
    }
    result
}

fn write_cbz_archive_inner(
    path: &Path,
    expected_pages: usize,
    comic_info_xml: String,
    receiver: mpsc::Receiver<(usize, String, Vec<u8>)>,
) -> eyre::Result<()> {
    if expected_pages == 0 {
        return Err(eyre!("章节没有可写入CBZ的图片"));
    }

    let mut writer_error = None;
    let mut writer = match File::create(path)
        .wrap_err(format!("创建临时CBZ`{}`失败", path.display()))
    {
        Ok(file) => Some(ZipWriter::new(file)),
        Err(err) => {
            writer_error = Some(err);
            None
        }
    };

    if let Some(archive) = writer.as_mut() {
        let result = archive
            .start_file("ComicInfo.xml", SimpleFileOptions::default())
            .wrap_err("在CBZ中创建`ComicInfo.xml`失败")
            .and_then(|()| {
                archive
                    .write_all(comic_info_xml.as_bytes())
                    .wrap_err("写入CBZ中的`ComicInfo.xml`失败")
            });
        if let Err(err) = result {
            writer_error = Some(err);
            writer = None;
        }
    }

    let mut next_page = 0;
    let mut pending_pages = BTreeMap::new();
    let mut receiver = receiver;
    while let Some((page_index, filename, image_data)) = receiver.blocking_recv() {
        if page_index >= expected_pages {
            if writer_error.is_none() {
                writer_error = Some(eyre!("图片页序号`{page_index}`超出预期页数`{expected_pages}`"));
                writer = None;
            }
            continue;
        }

        if writer.is_none() {
            continue;
        }
        pending_pages.insert(page_index, (filename, image_data));

        while let Some((filename, image_data)) = pending_pages.remove(&next_page) {
            let result = (|| -> eyre::Result<()> {
                let archive = writer
                    .as_mut()
                    .ok_or_eyre("CBZ写入器未初始化")?;
                archive
                    .start_file(&filename, SimpleFileOptions::default())
                    .wrap_err(format!("在CBZ中创建`{filename}`失败"))?;
                archive
                    .write_all(&image_data)
                    .wrap_err(format!("写入CBZ中的`{filename}`失败"))?;
                Ok(())
            })();

            if let Err(err) = result {
                writer_error = Some(err);
                writer = None;
                pending_pages.clear();
                break;
            }
            next_page += 1;
        }
    }

    if let Some(err) = writer_error {
        drop(writer);
        let _ = std::fs::remove_file(path);
        return Err(err);
    }
    if next_page != expected_pages {
        drop(writer);
        let _ = std::fs::remove_file(path);
        return Err(eyre!(
            "CBZ图片不完整：预期`{expected_pages}`页，实际写入`{next_page}`页"
        ));
    }

    writer
        .ok_or_eyre("CBZ写入器未初始化")?
        .finish()
        .wrap_err(format!("完成临时CBZ`{}`失败", path.display()))?;

    Ok(())
}
