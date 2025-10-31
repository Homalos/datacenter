<template>
  <div class="navbar" :class="{ 'navbar-dark': isDark }">
    <div class="navbar-content">
      <!-- Logo 和标题 -->
      <div class="navbar-brand">
        <n-text strong style="font-size: 20px">🚀 Homalos 数据中心</n-text>
      </div>

      <!-- 导航菜单 -->
      <div class="navbar-menu">
        <n-menu
          mode="horizontal"
          :value="activeKey"
          :options="menuOptions"
          @update:value="handleMenuSelect"
        />
      </div>

      <!-- 快捷链接 -->
      <div class="navbar-actions">
        <n-space :size="8">
          <n-button text tag="a" href="/docs" target="_blank" size="small">
            <template #icon>
              <n-icon><BookIcon /></n-icon>
            </template>
            文档
          </n-button>
          <n-button text tag="a" href="/health" target="_blank" size="small">
            <template #icon>
              <n-icon><HeartIcon /></n-icon>
            </template>
            健康
          </n-button>
        </n-space>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, h } from 'vue'
import { useRouter, useRoute } from 'vue-router'
import { storeToRefs } from 'pinia'
import { useThemeStore } from '@/stores/theme'
import { NIcon } from 'naive-ui'
import { HomeOutline as HomeIcon, InformationCircleOutline as AboutIcon, BookOutline as BookIcon, HeartOutline as HeartIcon } from '@vicons/ionicons5'

const router = useRouter()
const route = useRoute()
const themeStore = useThemeStore()
const { isDark } = storeToRefs(themeStore)

// 当前激活的菜单项
const activeKey = computed(() => route.name as string)

// 菜单选项
const menuOptions = [
  {
    label: '数据中心',
    key: 'Home',
    icon: () => h(NIcon, null, { default: () => h(HomeIcon) })
  },
  {
    label: '关于',
    key: 'About',
    icon: () => h(NIcon, null, { default: () => h(AboutIcon) })
  }
]

// 菜单选择处理
function handleMenuSelect(key: string) {
  router.push({ name: key })
}
</script>

<style scoped>
.navbar {
  background: #ffffff;
  border-bottom: 1px solid #e5e7eb;
  box-shadow: 0 1px 3px rgba(0, 0, 0, 0.05);
  position: sticky;
  top: 0;
  z-index: 100;
  transition: all 0.3s ease;
}

/* 暗黑模式 */
.navbar-dark {
  background: #1f2937 !important;
  border-bottom-color: #374151 !important;
  box-shadow: 0 1px 3px rgba(0, 0, 0, 0.3) !important;
}

.navbar-content {
  max-width: 1400px;
  margin: 0 auto;
  padding: 0 20px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  height: 60px;
  gap: 20px;
}

.navbar-brand {
  flex-shrink: 0;
}

.navbar-menu {
  flex: 1;
  display: flex;
  justify-content: center;
}

.navbar-actions {
  flex-shrink: 0;
}

/* 响应式 */
@media (max-width: 768px) {
  .navbar-content {
    padding: 0 15px;
    height: 56px;
  }
  
  .navbar-brand n-text {
    font-size: 18px !important;
  }
  
  .navbar-actions {
    display: none;
  }
}

@media (max-width: 480px) {
  .navbar-content {
    padding: 0 10px;
    height: 52px;
  }
  
  .navbar-brand n-text {
    font-size: 16px !important;
  }
}
</style>

