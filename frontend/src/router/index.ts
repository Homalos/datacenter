/**
 * Vue Router 配置
 */
import { createRouter, createWebHistory } from 'vue-router'
import type { RouteRecordRaw } from 'vue-router'

const routes: RouteRecordRaw[] = [
  {
    path: '/',
    name: 'Home',
    component: () => import('@/views/HomePage.vue'),
    meta: {
      title: '数据中心'
    }
  },
  {
    path: '/about',
    name: 'About',
    component: () => import('@/views/AboutPage.vue'),
    meta: {
      title: '关于'
    }
  }
]

const router = createRouter({
  history: createWebHistory('/dashboard'),  // ✅ 设置 base path 为 /dashboard
  routes
})

// 路由守卫：更新页面标题
router.beforeEach((to, _from, next) => {
  if (to.meta.title) {
    document.title = `${to.meta.title} - Homalos 数据中心`
  }
  next()
})

export default router

