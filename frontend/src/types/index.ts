/**
 * 数据中心类型定义
 */

// 服务状态
export type ServiceStatus = 'stopped' | 'starting' | 'running' | 'stopping' | 'error'

// 模块状态
export type ModuleStatus = 'pending' | 'starting' | 'running' | 'error' | 'registered'

// 日志级别
export type LogLevel = 'INFO' | 'WARNING' | 'ERROR' | 'DEBUG'

// 模块信息
export interface ModuleInfo {
  name: string
  status: ModuleStatus
  started_at?: string
  error_message?: string
}

// 服务状态
export interface ServiceState {
  status: ServiceStatus
  start_time?: string
  uptime_seconds: number
  modules: Record<string, ModuleInfo>
  error_message?: string
  last_update?: string
}

// 日志条目
export interface LogEntry {
  timestamp: string
  level: LogLevel
  message: string
  [key: string]: any
}

// API 响应
export interface ApiResponse<T = any> {
  code: number
  message: string
  data?: T
}

// 主题类型
export type ThemeMode = 'light' | 'dark' | 'auto'

