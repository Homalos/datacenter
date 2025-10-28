/**
 * Homalos 数据中心 - 控制面板脚本
 * 
 * 功能特性：
 * - 主题切换（暗黑/浅色模式）
 * - 实时状态刷新
 * - SSE 日志流
 * - 数据中心控制（启动/停止/重启）
 * - 响应式交互
 */

// ========================================
// 全局配置
// ========================================

let autoScroll = true;
let logEventSource = null;
let statusInterval = null;

// ========================================
// 页面初始化
// ========================================

document.addEventListener('DOMContentLoaded', function() {
    // 初始化主题
    initTheme();
    // 显示初始提醒日志
    showInitialLog();
    // 刷新状态
    refreshStatus();
    // 连接日志流
    connectLogStream();
    // 每3秒刷新一次状态
    statusInterval = setInterval(refreshStatus, 3000);
});

// ========================================
// 主题管理
// ========================================

/**
 * 初始化主题
 * 优先级：localStorage > 系统设置
 */
function initTheme() {
    // 从 localStorage 读取用户偏好
    const savedTheme = localStorage.getItem('theme');
    
    if (savedTheme === 'dark') {
        document.body.classList.add('dark-mode');
        updateThemeIcon('dark');
    } else if (savedTheme === 'light') {
        document.body.classList.add('light-mode');
        updateThemeIcon('light');
    } else {
        // 未保存偏好，使用系统设置
        if (window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches) {
            updateThemeIcon('dark');
        } else {
            updateThemeIcon('light');
        }
    }
}

/**
 * 切换主题
 */
function toggleTheme() {
    const body = document.body;
    const isDark = body.classList.contains('dark-mode');
    
    if (isDark) {
        // 切换到浅色
        body.classList.remove('dark-mode');
        body.classList.add('light-mode');
        localStorage.setItem('theme', 'light');
        updateThemeIcon('light');
        showAlert('已切换到浅色模式 ☀️', 'success');
    } else {
        // 切换到暗黑
        body.classList.remove('light-mode');
        body.classList.add('dark-mode');
        localStorage.setItem('theme', 'dark');
        updateThemeIcon('dark');
        showAlert('已切换到暗黑模式 🌙', 'success');
    }
}

/**
 * 更新主题图标
 * @param {string} theme - 'dark' 或 'light'
 */
function updateThemeIcon(theme) {
    const icon = document.getElementById('theme-icon');
    if (theme === 'dark') {
        icon.textContent = '☀️';  // 暗黑模式显示太阳图标（点击切换到浅色）
    } else {
        icon.textContent = '🌙';  // 浅色模式显示月亮图标（点击切换到暗黑）
    }
}

// ========================================
// 日志管理
// ========================================

/**
 * 显示初始提醒日志
 */
function showInitialLog() {
    const container = document.getElementById('logs-container');
    const now = new Date();
    const timestamp = now.toLocaleString('zh-CN', {
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hour12: false
    });
    
    const entry = document.createElement('div');
    entry.className = 'log-entry';
    entry.innerHTML = `
        <span class="log-timestamp">${timestamp}</span>
        <span class="log-level log-INFO">INFO</span>
        <span class="log-message">Web控制面板已加载，等待连接数据中心...</span>
    `;
    container.appendChild(entry);
}

/**
 * 连接日志流（SSE）
 */
function connectLogStream() {
    if (logEventSource) {
        logEventSource.close();
    }
    
    logEventSource = new EventSource('/datacenter/logs/stream');
    
    logEventSource.addEventListener('log', function(event) {
        try {
            // 验证数据有效性
            if (!event.data || event.data.trim() === '') {
                return;
            }
            
            const log = JSON.parse(event.data);
            
            // 验证日志对象有效性
            if (log && typeof log === 'object') {
                appendLog(log);
            }
        } catch (error) {
            // 静默处理 JSON 解析错误，避免控制台报错影响用户体验
            console.debug('跳过无效日志数据:', event.data, error.message);
        }
    });
    
    logEventSource.addEventListener('ping', function(event) {
        // 心跳，忽略
        // 可选：验证心跳数据
        try {
            if (event.data) {
                const ping = JSON.parse(event.data);
                console.debug('收到心跳:', ping);
            }
        } catch (error) {
            // 忽略心跳解析错误
        }
    });
    
    logEventSource.onerror = function(error) {
        console.error('日志流连接错误:', error);
        // 5秒后重连
        setTimeout(connectLogStream, 5000);
    };
}

/**
 * 添加日志条目
 * @param {Object} log - 日志对象 {timestamp, level, message}
 */
function appendLog(log) {
    const container = document.getElementById('logs-container');
    const entry = document.createElement('div');
    entry.className = 'log-entry';
    
    const timestamp = new Date(log.timestamp).toLocaleTimeString('zh-CN');
    const level = log.level || 'INFO';
    const message = log.message || '';
    
    entry.innerHTML = `
        <span class="log-timestamp">${timestamp}</span>
        <span class="log-level log-${level}">${level}</span>
        <span class="log-message">${escapeHtml(message)}</span>
    `;
    
    container.appendChild(entry);
    
    // 限制日志数量
    while (container.children.length > 500) {
        container.removeChild(container.firstChild);
    }
    
    // 自动滚动
    if (autoScroll) {
        container.scrollTop = container.scrollHeight;
    }
}

/**
 * 清空日志
 */
function clearLogs() {
    document.getElementById('logs-container').innerHTML = '';
}

/**
 * 切换自动滚动
 */
function toggleAutoScroll() {
    autoScroll = !autoScroll;
    document.getElementById('autoscroll-icon').textContent = autoScroll ? '✅' : '❌';
}

// ========================================
// 状态管理
// ========================================

/**
 * 刷新状态
 */
async function refreshStatus() {
    try {
        const response = await fetch('/datacenter/status');
        const result = await response.json();
        
        if (result.code === 0) {
            updateUI(result.data);
        }
    } catch (error) {
        console.error('刷新状态失败:', error);
    }
}

/**
 * 更新UI
 * @param {Object} data - 状态数据
 */
function updateUI(data) {
    // 更新状态徽章
    const statusBadge = document.getElementById('status-badge');
    statusBadge.className = 'status-badge status-' + data.status;
    statusBadge.textContent = getStatusText(data.status);
    
    // 更新启动时间
    document.getElementById('start-time').textContent = 
        data.start_time ? new Date(data.start_time).toLocaleString('zh-CN') : '-';
    
    // 更新运行时长
    document.getElementById('uptime').textContent = formatUptime(data.uptime_seconds);
    
    // 更新模块列表
    updateModulesList(data.modules || {});
    
    // 显示错误信息
    if (data.error_message) {
        showAlert('错误: ' + data.error_message, 'error');
    }
}

/**
 * 更新模块列表
 * @param {Object} modules - 模块对象 {name: {status, ...}}
 */
function updateModulesList(modules) {
    const container = document.getElementById('modules-list');
    
    if (Object.keys(modules).length === 0) {
        container.innerHTML = '<p style="text-align: center; color: #999; padding: 40px 0;">暂无模块信息</p>';
        return;
    }
    
    let html = '';
    for (const [name, info] of Object.entries(modules)) {
        html += `
            <div class="module-item">
                <span class="module-name">${name}</span>
                <span class="module-status module-${info.status}">${info.status}</span>
            </div>
        `;
    }
    container.innerHTML = html;
}

// ========================================
// 数据中心控制
// ========================================

/**
 * 启动数据中心
 */
async function startDatacenter() {
    try {
        const response = await fetch('/datacenter/start', { method: 'POST' });
        const result = await response.json();
        
        if (result.code === 0) {
            showAlert('数据中心启动命令已发送，请查看实时日志...', 'success');
        } else {
            showAlert('启动失败: ' + result.message, 'error');
        }
        
        refreshStatus();
    } catch (error) {
        showAlert('启动失败: ' + error.message, 'error');
    }
}

/**
 * 停止数据中心
 */
async function stopDatacenter() {
    if (!confirm('确定要停止数据中心吗？')) {
        return;
    }
    
    try {
        const response = await fetch('/datacenter/stop', { method: 'POST' });
        const result = await response.json();
        
        if (result.code === 0) {
            showAlert('数据中心停止命令已发送', 'success');
        } else {
            showAlert('停止失败: ' + result.message, 'error');
        }
        
        refreshStatus();
    } catch (error) {
        showAlert('停止失败: ' + error.message, 'error');
    }
}

/**
 * 重启数据中心
 */
async function restartDatacenter() {
    if (!confirm('确定要重启数据中心吗？')) {
        return;
    }
    
    try {
        const response = await fetch('/datacenter/restart', { method: 'POST' });
        const result = await response.json();
        
        if (result.code === 0) {
            showAlert('数据中心重启命令已发送', 'success');
        } else {
            showAlert('重启失败: ' + result.message, 'error');
        }
        
        refreshStatus();
    } catch (error) {
        showAlert('重启失败: ' + error.message, 'error');
    }
}

// ========================================
// UI 反馈
// ========================================

/**
 * 显示提示
 * @param {string} message - 提示消息
 * @param {string} type - 'success' 或 'error'
 */
function showAlert(message, type = 'success') {
    const container = document.getElementById('alert-container');
    const alert = document.createElement('div');
    alert.className = `alert alert-${type}`;
    alert.textContent = message;
    
    container.appendChild(alert);
    
    setTimeout(() => {
        alert.remove();
    }, 5000);
}

// ========================================
// 工具函数
// ========================================

/**
 * 获取状态文本
 * @param {string} status - 状态代码
 * @returns {string} 状态文本
 */
function getStatusText(status) {
    const statusMap = {
        'stopped': '⚫ 未启动',
        'starting': '🟡 启动中',
        'running': '🟢 运行中',
        'stopping': '🟠 停止中',
        'error': '🔴 错误'
    };
    return statusMap[status] || status;
}

/**
 * 格式化运行时长
 * @param {number} seconds - 秒数
 * @returns {string} 格式化的时长
 */
function formatUptime(seconds) {
    if (!seconds) return '0秒';
    
    const days = Math.floor(seconds / 86400);
    const hours = Math.floor((seconds % 86400) / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    const secs = seconds % 60;
    
    let result = '';
    if (days > 0) result += `${days}天 `;
    if (hours > 0) result += `${hours}小时 `;
    if (minutes > 0) result += `${minutes}分 `;
    result += `${secs}秒`;
    
    return result;
}

/**
 * HTML 转义
 * @param {string} text - 待转义文本
 * @returns {string} 转义后的文本
 */
function escapeHtml(text) {
    const map = {
        '&': '&amp;',
        '<': '&lt;',
        '>': '&gt;',
        '"': '&quot;',
        "'": '&#039;'
    };
    return text.replace(/[&<>"']/g, m => map[m]);
}

// ========================================
// 页面清理
// ========================================

/**
 * 页面卸载时清理资源
 */
window.addEventListener('beforeunload', function() {
    if (logEventSource) {
        logEventSource.close();
    }
    if (statusInterval) {
        clearInterval(statusInterval);
    }
});

