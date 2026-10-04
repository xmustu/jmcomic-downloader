<script setup lang="ts">
import { ref, watch } from 'vue'
import { commands } from '../../bindings.ts'
import { useStore } from '../../store.ts'
import { PhFolderOpen } from '@phosphor-icons/vue'
import IconButton from '../../components/IconButton.vue'
import ChapterDownloadPanel from './components/ChapterDownloadPanel.vue'
import ChapterExportPanel from './components/ChapterExportPanel.vue'
import { NEmpty, NImage } from 'naive-ui'

export type ChapterPaneMode = 'download' | 'export'

const store = useStore()

const chapterPaneMode = ref<ChapterPaneMode>('download')
const previewImages = ref<string[]>([])
let previewRequestId = 0

function removeFailedPreview(previewImage: string) {
  previewImages.value = previewImages.value.filter((image) => image !== previewImage)
}

watch(
  () => store.pickedComic,
  () => {
    chapterPaneMode.value = 'download'
  },
)

watch(
  () => store.pickedComic?.id,
  async (comicId) => {
    const requestId = ++previewRequestId
    previewImages.value = []

    const comic = store.pickedComic
    const firstChapter = comic?.chapterInfos[0]
    if (comicId === undefined || firstChapter === undefined) {
      return
    }

    try {
      const result = await commands.getChapterPreviewImages(firstChapter.chapterId)
      if (requestId !== previewRequestId || store.pickedComic?.id !== comicId) {
        return
      }
      if (result.status === 'error') {
        console.error(result.error)
        return
      }

      previewImages.value = result.data
    } catch (error) {
      if (requestId === previewRequestId) {
        console.error(error)
      }
    }
  },
  { immediate: true },
)

async function reloadPickedComic() {
  if (store.pickedComic === undefined) {
    return
  }

  const result = await commands.getComic(store.pickedComic.id)
  if (result.status === 'error') {
    console.error(result.error)
    return
  }

  store.pickedComic = result.data
}

async function showComicDownloadDirInFileManager() {
  if (store.pickedComic === undefined) {
    return
  }

  const comicDownloadDir = store.pickedComic.comicDownloadDir
  if (comicDownloadDir === undefined || comicDownloadDir === null) {
    console.error('comicDownloadDir的值为undefined或null')
    return
  }

  const result = await commands.showPathInFileManager(comicDownloadDir)
  if (result.status === 'error') {
    console.error(result.error)
  }
}
</script>

<template>
  <div class="h-full flex flex-col box-border">
    <n-empty v-if="store.pickedComic === undefined" description="请先选择漫画(搜索、收藏夹、每周必看、本地库存)" />
    <template v-else>
      <ChapterDownloadPanel
        v-if="chapterPaneMode === 'download'"
        v-model:chapter-pane-mode="chapterPaneMode"
        :reload="reloadPickedComic" />
      <ChapterExportPanel v-else v-model:chapter-pane-mode="chapterPaneMode" :reload="reloadPickedComic" />

      <div class="flex items-start p-2 pt-0">
        <div class="mr-4 flex w-24 shrink-0 flex-col gap-1">
          <img
            class="w-24 object-cover"
            :src="`https://cdn-msp3.18comic.vip/media/albums/${store.pickedComic.id}_3x4.jpg`"
            alt=""
            referrerpolicy="no-referrer" />
          <div v-if="previewImages.length > 0" class="grid w-full grid-cols-3 gap-1">
            <n-image
              v-for="(previewImage, index) in previewImages"
              :key="`${store.pickedComic.id}-${index}`"
              :src="previewImage"
              :preview-src="previewImage"
              :alt="`${store.pickedComic.name}预览图${index + 1}`"
              width="29"
              height="40"
              object-fit="cover"
              @error="removeFailedPreview(previewImage)" />
          </div>
        </div>
        <div class="flex flex-col w-full">
          <span class="font-bold text-lg line-clamp-2">{{ store.pickedComic.name }}</span>
          <span class="text-red">作者：{{ store.pickedComic.author }}</span>
          <span class="text-gray">标签：{{ store.pickedComic.tags }}</span>
          <IconButton
            v-if="store.pickedComic.isDownloaded"
            class="mt-auto mr-auto"
            title="打开下载目录"
            @click="showComicDownloadDirInFileManager">
            <PhFolderOpen :size="24" />
          </IconButton>
        </div>
      </div>
    </template>
  </div>
</template>
