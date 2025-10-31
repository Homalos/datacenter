/**
 * 数据中心 API 服务
 */
import axios, { AxiosInstance } from 'axios'
import type { ApiResponse, ServiceState, LogEntry } from '@/types'

class DatacenterApi {
  private client: AxiosInstance

  constructor() {
    this.client = axios.create({
      baseURL: '/datacenter',
      timeout: 10000,
      headers: {
        'Content-Type': 'application/json'
      }
    })

    // 响应拦截器
    this.client.interceptors.response.use(
      (response) => response,
      (error) => {
        console.error('API 请求失败:', error)
        return Promise.reject(error)
      }
    )
  }

  /**
   * 获取数据中心状态
   */
  async getStatus(): Promise<ServiceState> {
    const { data } = await this.client.get<ApiResponse<ServiceState>>('/status')
    return data.data!
  }

  /**
   * 启动数据中心
   */
  async start(): Promise<ApiResponse<ServiceState>> {
    const { data } = await this.client.post<ApiResponse<ServiceState>>('/start')
    return data
  }

  /**
   * 停止数据中心
   */
  async stop(): Promise<ApiResponse<ServiceState>> {
    const { data } = await this.client.post<ApiResponse<ServiceState>>('/stop')
    return data
  }

  /**
   * 重启数据中心
   */
  async restart(): Promise<ApiResponse<ServiceState>> {
    const { data } = await this.client.post<ApiResponse<ServiceState>>('/restart')
    return data
  }

  /**
   * 获取日志列表
   */
  async getLogs(limit: number = 100): Promise<LogEntry[]> {
    const { data } = await this.client.get<ApiResponse<LogEntry[]>>('/logs', {
      params: { limit }
    })
    return data.data!
  }

  /**
   * 创建 SSE 日志流连接
   */
  createLogStream(): EventSource {
    return new EventSource('/datacenter/logs/stream')
  }
}

// 导出单例
export const datacenterApi = new DatacenterApi()

