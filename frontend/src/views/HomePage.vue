<template>
  <div class="home-page">
    <n-grid :cols="2" :x-gap="20" :y-gap="20" responsive="screen" :collapsed-rows="1">
      <!-- 状态卡片 -->
      <n-gi>
        <StatusCard />
      </n-gi>

      <!-- 模块卡片 -->
      <n-gi>
        <ModulesCard />
      </n-gi>

      <!-- 日志卡片（占满整行） -->
      <n-gi :span="2">
        <LogsCard />
      </n-gi>
    </n-grid>
  </div>
</template>

<script setup lang="ts">
import { onMounted } from 'vue'
import { useDatacenterStore } from '@/stores/datacenter'
import StatusCard from '@/components/StatusCard.vue'
import ModulesCard from '@/components/ModulesCard.vue'
import LogsCard from '@/components/LogsCard.vue'

const datacenterStore = useDatacenterStore()

// 初始化
onMounted(() => {
  datacenterStore.fetchStatus()

  // 每3秒刷新一次状态
  setInterval(() => {
    datacenterStore.fetchStatus()
  }, 3000)
})
</script>

<style scoped>
.home-page {
  width: 100%;
}

/* 响应式 */
@media (max-width: 768px) {
  :deep(.n-grid) {
    grid-template-columns: 1fr !important;
  }
  
  :deep(.n-grid > .n-grid-item) {
    grid-column: span 1 !important;
  }
}
</style>

