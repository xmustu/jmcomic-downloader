use std::{
    io::Cursor,
    ops::ControlFlow,
    path::PathBuf,
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

use bytes::Bytes;
use eyre::WrapErr;
use image::codecs::png;
use image::codecs::png::PngEncoder;
use image::{ImageFormat, RgbImage};
use tauri::AppHandle;
use tokio::{
    sync::{mpsc, watch, SemaphorePermit},
    time::sleep,
};
use tracing::instrument;

use crate::{
    downloader::{download_task::DownloadTask, download_task_state::DownloadTaskState},
    extensions::{AppHandleExt, EyreReportToMessage},
    types::DownloadFormat,
    utils,
};

pub struct DownloadImgTask {
    app: AppHandle,
    download_task: Arc<DownloadTask>,
    url: String,
    index: usize,
    output: DownloadImgOutput,
    block_num: u32,
}

pub enum DownloadImgOutput {
    Directory(PathBuf),
    Cbz(mpsc::Sender<(usize, String, Vec<u8>)>),
}

impl DownloadImgTask {
    pub fn new(
        download_task: Arc<DownloadTask>,
        url: String,
        index: usize,
        output: DownloadImgOutput,
        block_num: u32,
    ) -> Self {
        Self {
            app: download_task.app.clone(),
            download_task,
            url,
            index,
            output,
            block_num,
        }
    }

    #[instrument(
        level = "error",
        skip_all,
        fields(
            index = self.index,
            url = self.url,
            comic_id = self.download_task.comic.id,
            comic_title = self.download_task.comic.name,
            chapter_id = self.download_task.chapter_info.chapter_id,
            chapter_title = self.download_task.chapter_info.chapter_title
        )
    )]
    pub async fn process(self) {
        let download_img_task = self.download_img();
        tokio::pin!(download_img_task);

        let mut state_receiver = self.download_task.state_sender.subscribe();
        state_receiver.mark_changed();

        let mut delete_receiver = self.download_task.delete_sender.subscribe();

        let mut permit = None;

        loop {
            let state_is_downloading = *state_receiver.borrow() == DownloadTaskState::Downloading;
            tokio::select! {
                result = &mut download_img_task, if state_is_downloading && permit.is_some() => {
                    if let Err(err) = result {
                        let err_title = "处理并保存图片失败";
                        let message = err.to_message();
                        tracing::error!(err_title, message);
                    }
                    break;
                },

                control_flow = self.acquire_img_permit(&mut permit), if state_is_downloading && permit.is_none() => {
                    match control_flow {
                        ControlFlow::Continue(()) => {}
                        ControlFlow::Break(()) => break,
                    }
                }

                _ = state_receiver.changed() => {
                    match self.handle_state_change(&mut permit, &mut state_receiver).await {
                        ControlFlow::Continue(()) => {}
                        ControlFlow::Break(()) => break,
                    }
                }

                _ = delete_receiver.changed() => {
                    self.handle_delete_receiver_change(&mut permit).await;
                    return;
                }
            }
        }
    }

    #[instrument(level = "error", skip_all)]
    async fn download_img(&self) -> eyre::Result<()> {
        let url = &self.url;

        let index_filename = format!("{:04}", self.index + 1);
        let download_format = self.app.get_config().read().download_format;
        let extension = download_format.extension();

        match &self.output {
            DownloadImgOutput::Directory(temp_download_dir) => {
                let user_format_path =
                    temp_download_dir.join(format!("{index_filename}.{extension}"));
                let gif_path = temp_download_dir.join(format!("{index_filename}.gif"));
                if user_format_path.exists() || gif_path.exists() {
                    self.download_task
                        .downloaded_img_count
                        .fetch_add(1, Ordering::Relaxed);
                    self.download_task.emit_download_task_update_event();
                    tracing::trace!("图片已存在，跳过下载");
                    return Ok(());
                }
            }
            DownloadImgOutput::Cbz(_) => {}
        }

        tracing::trace!("开始下载图片");

        let (img_data, format) = self
            .app
            .get_jm_client()
            .get_img_data_and_format(url)
            .await
            .wrap_err("下载图片失败")?;
        let img_data_len = img_data.len() as u64;

        tracing::trace!("图片成功下载到内存");

        let filename = if format == ImageFormat::Gif {
            format!("{index_filename}.gif")
        } else {
            format!("{index_filename}.{extension}")
        };
        let image_data = process_img(download_format, self.block_num, img_data, format)
            .await
            .wrap_err("图片解码、解密或编码失败")?;

        match &self.output {
            DownloadImgOutput::Directory(temp_download_dir) => {
                let save_path = temp_download_dir.join(filename);
                std::fs::write(&save_path, image_data)
                    .wrap_err(format!("保存图片`{}`失败", save_path.display()))?;
            }
            DownloadImgOutput::Cbz(sender) => sender
                .send((self.index, filename, image_data))
                .await
                .map_err(|_| eyre::eyre!("CBZ写入通道已关闭"))?,
        }

        tracing::trace!("图片已保存");

        self.app
            .get_download_manager()
            .byte_per_sec
            .fetch_add(img_data_len, Ordering::Relaxed);

        self.download_task
            .downloaded_img_count
            .fetch_add(1, Ordering::Relaxed);

        self.download_task.emit_download_task_update_event();

        let img_download_interval_sec = self.app.get_config().read().img_download_interval_sec;
        sleep(Duration::from_secs(img_download_interval_sec)).await;
        Ok(())
    }

    #[instrument(level = "error", skip_all)]
    async fn acquire_img_permit<'a>(
        &'a self,
        permit: &mut Option<SemaphorePermit<'a>>,
    ) -> ControlFlow<()> {
        tracing::trace!("图片开始排队");

        *permit = match permit.take() {
            Some(permit) => Some(permit),
            None => match self
                .app
                .get_download_manager()
                .inner()
                .img_sem
                .acquire()
                .await
                .map_err(eyre::Report::from)
            {
                Ok(permit) => Some(permit),
                Err(err) => {
                    let err_title = "获取下载图片的permit失败";
                    let message = err.to_message();
                    tracing::error!(err_title, message);
                    return ControlFlow::Break(());
                }
            },
        };

        ControlFlow::Continue(())
    }

    #[instrument(level = "error", skip_all)]
    async fn handle_state_change<'a>(
        &'a self,
        permit: &mut Option<SemaphorePermit<'a>>,
        state_receiver: &mut watch::Receiver<DownloadTaskState>,
    ) -> ControlFlow<()> {
        let state = *state_receiver.borrow();
        if state == DownloadTaskState::Paused {
            sleep(Duration::from_millis(100)).await;
            tracing::trace!("图片暂停下载");
            if let Some(permit) = permit.take() {
                drop(permit);
            }
        } else if state == DownloadTaskState::Failed {
            sleep(Duration::from_millis(100)).await;
            tracing::trace!("图片取消下载");
            if let Some(permit) = permit.take() {
                drop(permit);
            }
        }

        ControlFlow::Continue(())
    }

    #[instrument(level = "error", skip_all)]
    async fn handle_delete_receiver_change<'a>(&'a self, permit: &mut Option<SemaphorePermit<'a>>) {
        if permit.is_some() {
            sleep(Duration::from_millis(100)).await;
        }

        tracing::trace!("图片取消下载");
    }
}

pub fn calculate_block_num(scramble_id: i64, id: i64, filename: &str) -> u32 {
    if id < scramble_id {
        0
    } else if id < 268_850 {
        10
    } else {
        let x = if id < 421_926 { 10 } else { 8 };
        let s = format!("{id}{filename}");
        let s = utils::md5_hex(&s);
        let mut block_num = s.chars().last().unwrap() as u32;
        block_num %= x;
        block_num = block_num * 2 + 2;
        block_num
    }
}

#[instrument(
    level = "error",
    skip_all,
    fields(
        download_format = ?download_format,
        block_num = block_num,
        src_format = ?src_format
    )
)]
pub(crate) async fn process_img(
    download_format: DownloadFormat,
    block_num: u32,
    src_img_data: Bytes,
    src_format: ImageFormat,
) -> eyre::Result<Vec<u8>> {
    if src_format == ImageFormat::Gif {
        return Ok(src_img_data.to_vec());
    }

    let current_span = tracing::Span::current();
    let process_img = move || -> eyre::Result<Vec<u8>> {
        let _enter = current_span.enter();
        let mut src_img = image::load_from_memory(&src_img_data)
            .wrap_err("解码图片失败")?
            .to_rgb8();

        let dst_img = if block_num == 0 {
            src_img
        } else {
            stitch_img(&mut src_img, block_num)
        };

        let mut dst_img_data = Vec::new();
        match download_format {
            DownloadFormat::Jpeg => {
                dst_img.write_to(&mut Cursor::new(&mut dst_img_data), ImageFormat::Jpeg)?;
            }
            DownloadFormat::Png => {
                let encoder = PngEncoder::new_with_quality(
                    Cursor::new(&mut dst_img_data),
                    png::CompressionType::Best,
                    png::FilterType::default(),
                );
                dst_img.write_with_encoder(encoder)?;
            }
            DownloadFormat::Webp => {
                dst_img.write_to(&mut Cursor::new(&mut dst_img_data), ImageFormat::WebP)?;
            }
        }

        Ok(dst_img_data)
    };

    let (sender, receiver) = tokio::sync::oneshot::channel::<eyre::Result<Vec<u8>>>();
    rayon::spawn(move || {
        let _ = sender.send(process_img());
    });

    let result = receiver.await?;
    result
}

fn stitch_img(src_img: &mut RgbImage, block_num: u32) -> RgbImage {
    let (width, height) = src_img.dimensions();
    let mut stitched_img = image::ImageBuffer::new(width, height);
    let remainder_height = height % block_num;
    for i in 0..block_num {
        let mut block_height = height / block_num;
        let src_img_y_start = height - (block_height * (i + 1)) - remainder_height;
        let mut dst_img_y_start = block_height * i;
        if i == 0 {
            block_height += remainder_height;
        } else {
            dst_img_y_start += remainder_height;
        }

        for y in 0..block_height {
            let src_y = src_img_y_start + y;
            let dst_y = dst_img_y_start + y;
            for x in 0..width {
                stitched_img.put_pixel(x, dst_y, *src_img.get_pixel(x, src_y));
            }
        }
    }

    stitched_img
}
