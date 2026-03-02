//#region ==================== 全局变量初始化 ====================
// 确保全局变量存在
if (typeof isSending === 'undefined') window.isSending = false;
if (typeof globalStats === 'undefined') window.globalStats = { taskCount: 0, totalSent: 0, totalSuccess: 0, totalFail: 0, totalTime: 0, totalPhoneCount: 0, inboxReceived: 0, inboxSent: 0, inboxTotal: 0 };
if (typeof currentTaskId === 'undefined') window.currentTaskId = null;

// 全局变量访问器
function getCurrentUserId() { return window.currentUserId; }
function getAuthToken() { return window.authToken; }
function getActiveWs() { return window.activeWs; }

function _formatStatusTime(date = new Date()) {
    const hh = String(date.getHours()).padStart(2, '0');
    const mm = String(date.getMinutes()).padStart(2, '0');
    return `${hh}:${mm}`;
}

function addStatusMessage(message, type = 'info') {
    const raw = String(message ?? '').replace(/\[INFO\]\s*/gi, '').trim();
    if (!raw) return;

    const line = /^\[\d{1,2}:\d{2}\]/.test(raw) ? raw : `[${_formatStatusTime()}] ${raw}`;
    const applyColor = (el) => {
        if (type === 'error') el.style.color = '#d32f2f';
        else if (type === 'warning') el.style.color = '#ef6c00';
        else if (type === 'success') el.style.color = '#2e7d32';
    };

    const appendOne = (container) => {
        if (!container) return;
        const item = document.createElement('div');
        item.className = `log-item log-${type}`;
        item.textContent = line;
        applyColor(item);
        container.appendChild(item);
        while (container.children.length > 500) {
            container.removeChild(container.firstChild);
        }
        container.scrollTop = container.scrollHeight;
    };

    appendOne(document.getElementById('statusList'));
    appendOne(document.getElementById('statusListMobile'));
}

function toFiniteNumber(value, fallback = 0) {
    const n = Number(value);
    return Number.isFinite(n) ? n : fallback;
}

function normalizeTaskResult(raw) {
    const src = (raw && typeof raw === 'object') ? raw : {};
    const success = toFiniteNumber(
        src.success ?? src.success_count ?? src.ok_count ?? src.send_ok ?? src.succeed,
        0
    );
    const fail = toFiniteNumber(
        src.fail ?? src.failed ?? src.fail_count ?? src.send_fail,
        0
    );
    const sent = toFiniteNumber(
        src.sent ?? src.total ?? src.total_sent ?? (success + fail),
        success + fail
    );
    return { success, fail, sent };
}

function renderFailedDetails(details) {
    const rows = Array.isArray(details) ? details.slice(0, 500).map((d) => ({
        phone: String((d && d.phone) || '-'),
        code: String((d && d.code) || 'CODE_UNKNOWN'),
        reason: String((d && d.reason) || '未知原因')
    })) : [];
    _latestFailedDetails = rows;

    const failedBtn = document.getElementById('failedDetailsBtn');
    const failedBtnMobile = document.getElementById('failedDetailsBtnMobile');
    if (failedBtn) failedBtn.style.display = rows.length > 0 ? '' : 'none';
    if (failedBtnMobile) failedBtnMobile.style.display = rows.length > 0 ? '' : 'none';
    _toggleFailedPanel(false);

    const listEl = document.getElementById('failedDetailsList');
    const listElMobile = document.getElementById('failedDetailsListMobile');
    if (!listEl || !listElMobile) return;

    if (rows.length === 0) {
        listEl.textContent = '无失败号码';
        listElMobile.textContent = '无失败号码';
        return;
    }

    const pW = Math.max(8, ...rows.map(r => r.phone.length));
    const cW = Math.max(8, ...rows.map(r => r.code.length));
    const lines = [];
    lines.push('失败号码详情');
    lines.push(`${'号码'.padEnd(pW)}  ${'失败CODE'.padEnd(cW)}  原因`);
    for (const r of rows) {
        lines.push(`${r.phone.padEnd(pW)}  ${r.code.padEnd(cW)}  ${r.reason}`);
    }
    const text = lines.join('\n');
    listEl.textContent = text;
    listElMobile.textContent = text;
}

const _taskProgressCache = new Map();
let _latestFailedDetails = [];
let _latestTaskSummary = null;

function _fmtClockNow() {
    return _formatStatusTime();
}

function _makeProgressBar(percent) {
    const width = 10;
    const filled = Math.max(0, Math.min(width, Math.round((Number(percent) || 0) / 10)));
    return '▓'.repeat(filled) + '/'.repeat(width - filled);
}

function _toggleFailedPanel(show) {
    const statusList = document.getElementById('statusList');
    const failedPanel = document.getElementById('failedDetailsPanel');
    const failedActions = document.getElementById('failedActions');
    const failedBtn = document.getElementById('failedDetailsBtn');
    const clearBtn = document.getElementById('clearLogsBtn');
    if (statusList && failedPanel) {
        statusList.style.display = show ? 'none' : '';
        failedPanel.style.display = show ? '' : 'none';
    }
    if (failedActions) failedActions.style.display = show ? 'flex' : 'none';
    if (clearBtn) clearBtn.style.display = show ? 'none' : '';
    if (failedBtn) failedBtn.style.display = show || _latestFailedDetails.length === 0 ? 'none' : '';

    const statusListMobile = document.getElementById('statusListMobile');
    const failedPanelMobile = document.getElementById('failedDetailsPanelMobile');
    const failedActionsMobile = document.getElementById('failedActionsMobile');
    const failedBtnMobile = document.getElementById('failedDetailsBtnMobile');
    const clearBtnMobile = document.getElementById('clearLogsBtnMobile');
    const backToSendBtn = document.getElementById('backToSendBtn');
    if (statusListMobile && failedPanelMobile) {
        statusListMobile.style.display = show ? 'none' : '';
        failedPanelMobile.style.display = show ? '' : 'none';
    }
    if (failedActionsMobile) failedActionsMobile.style.display = show ? 'flex' : 'none';
    if (clearBtnMobile) clearBtnMobile.style.display = show ? 'none' : '';
    if (backToSendBtn) backToSendBtn.style.display = show ? 'none' : '';
    if (failedBtnMobile) failedBtnMobile.style.display = show || _latestFailedDetails.length === 0 ? 'none' : '';
}

function _bindFailedPanelEvents() {
    if (window.__failedPanelBound) return;
    window.__failedPanelBound = true;

    const showBtns = ['failedDetailsBtn', 'failedDetailsBtnMobile'];
    showBtns.forEach((id) => {
        const btn = document.getElementById(id);
        if (btn) btn.addEventListener('click', () => _toggleFailedPanel(true));
    });

    const backBtns = ['failedBackBtn', 'failedBackBtnMobile'];
    backBtns.forEach((id) => {
        const btn = document.getElementById(id);
        if (btn) btn.addEventListener('click', () => _toggleFailedPanel(false));
    });

    const importHandler = () => {
        const numbers = _latestFailedDetails.map((x) => String((x && x.phone) || '').trim()).filter(Boolean);
        const numbersText = document.getElementById('numbersText');
        if (numbersText) {
            numbersText.value = numbers.join('\n');
            if (typeof updateCounts === 'function') updateCounts();
        }
    };
    ['failedImportBtn', 'failedImportBtnMobile'].forEach((id) => {
        const btn = document.getElementById(id);
        if (btn) btn.addEventListener('click', importHandler);
    });

    const saveHandler = () => {
        try {
            localStorage.setItem('last_failed_details', JSON.stringify(_latestFailedDetails || []));
            localStorage.setItem('last_task_summary', JSON.stringify(_latestTaskSummary || {}));
            addStatusMessage('失败号码已保存到用户面板数据缓存', 'success');
        } catch (e) {
            addStatusMessage(`保存失败: ${e.message || e}`, 'error');
        }
    };
    ['failedSaveBtn', 'failedSaveBtnMobile'].forEach((id) => {
        const btn = document.getElementById(id);
        if (btn) btn.addEventListener('click', saveHandler);
    });
}

/** 创建发送任务 */
async function _createTask({ message, numbers, taskType = 'normal' }) {
    const userId = getCurrentUserId();
    if (!userId) {
        throw new Error('用户未登录');
    }

    const token = getAuthToken() || StorageManager.session.getUserToken();
    if (!token) {
        throw new Error('认证令牌无效，请重新登录');
    }

    const response = await fetch(`${API_BASE_URL}/task/create`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${token}`
        },
        body: JSON.stringify({
            user_id: userId,
            message: message,
            numbers: numbers,
            task_type: taskType
        })
    });

    if (!response.ok) {
        const errorData = await response.json().catch(() => ({}));
        throw new Error(errorData.message || `创建任务失败: ${response.status}`);
    }

    const data = await response.json();
    if (!data.success && !data.ok) {
        throw new Error(data.message || '创建任务失败');
    }

    return data.task_id || data.taskId;
}
//#endregion

//#region ==================== WebSocket消息处理模块 ====================

// 处理服务器推送的消息（任务状态、余额更新、收件箱等）
function handleServerMessage(data, serverId = null) {
    if (!data || typeof data !== 'object') return;

    if (data.type === 'auth' && data.ok) {
        if (typeof sendWSCommand === 'function') {
            try { sendWSCommand('subscribe_servers', {}); } catch { /* ignore */ }
        }
        return;
    }

    if (data.type === 'balance_update') {
        const newBalance = data.balance !== undefined ? data.balance : data.credits;
        if (newBalance !== undefined) {
            if (typeof updateUserInfoDisplay === 'function') {
                updateUserInfoDisplay(newBalance);
            }
            // 同时更新currentCredits元素的dataset.raw（用于切换显示模式）
            const balanceEl = document.getElementById('currentCredits');
            if (balanceEl) {
                balanceEl.dataset.raw = newBalance;
            }
        }
        return;
    }

    if (data.type === 'usage_update' || data.type === 'usage_records_update') {
        if (data.records || data.usage_records) {
            if (typeof updateUsageRecordsDisplay === 'function') {
                updateUsageRecordsDisplay(data.records || data.usage_records);
            }
        }
        return;
    }

    if (data.type === 'access_update' || data.type === 'access_records_update') {
        if (data.records || data.access_records) {
            if (typeof updateAccessRecordsDisplay === 'function') {
                updateAccessRecordsDisplay(data.records || data.access_records);
            }
        }
        return;
    }

    if (data.type === 'servers_event') {
        try { loadServersFromAPI(); } catch { /* ignore */ }
        try { loadExclusivePhoneNumbers(); } catch { /* ignore */ }
        return;
    }

    if (data.type === 'balance_update' && data.balance !== undefined) {
        try { updateUserInfoDisplay(data.balance); } catch { /* ignore */ }
        return;
    }

    if (data.type === 'usage_update' && data.usage_records) {
        return;
    }

    if (data.type === 'access_update' && data.access_records) {
        return;
    }

    if (data.type === 'task_update') {
        const LOCATION = '[dadfunction.js][handleServerMessage]';
        const payload = (data.data && typeof data.data === 'object') ? data.data : data;
        if (!payload || typeof payload !== 'object') return;
        const taskId = payload.task_id || data.task_id;
        const traceId = payload.trace_id || (taskId ? (StorageManager.task.getTraceId(taskId) || '') : '');
        const sc = payload.shards || {};
        const rp = normalizeTaskResult(payload.result || payload.task_result || payload.result_data || {});
        const shardDone = Number(sc.done || 0);
        const shardTotal = Number(sc.total || 0);
        const shardsComplete = shardTotal > 0 && shardDone >= shardTotal;
        const phase = String(payload.phase || (payload.detail && payload.detail.phase) || '').toLowerCase();

        // 如果是当前追踪的任务，记录进度
        if (taskTracker.currentTaskId === taskId) {
            if (payload.status === 'pending') {
                taskTracker.logStep('📤 分片分配', `trace_id=${traceId} 待处理: ${sc.pending || 0}, 运行中: ${sc.running || 0}, 已完成: ${sc.done || 0}/${sc.total || 0}`, LOCATION);
            } else if (payload.status === 'running') {
                taskTracker.logStep('⚙️ Worker处理中', `trace_id=${traceId} 已完成: ${sc.done || 0}/${sc.total || 0}, 成功: ${rp.success}, 失败: ${rp.fail}`, LOCATION);
            }
        }

        if (payload.status === 'pending' || payload.status === 'running') {
            const totalShards = Number(sc.total || 0);
            const doneShards = Number(sc.done || 0);
            if (taskId && totalShards > 0) {
                const cache = _taskProgressCache.get(taskId) || { done: -1 };
                if (doneShards !== cache.done) {
                    const percent = Math.min(100, Math.floor((doneShards * 100) / totalShards));
                    addStatusMessage(`[${_fmtClockNow()}] 任务完成:      ${_makeProgressBar(percent)}      ${percent}%`, 'info');
                    _taskProgressCache.set(taskId, { done: doneShards, total: totalShards });
                }
            }
            if (payload.phase_message && String(payload.phase_message).includes('网络不稳定')) {
                addStatusMessage(payload.phase_message, 'warning');
            }
        }

        if (payload.status === 'done') {
            if (taskTracker.currentTaskId === taskId) {
                taskTracker.logStep('✅ 所有分片完成', `trace_id=${traceId} 成功: ${rp.success}, 失败: ${rp.fail}, 总计: ${rp.sent}`, LOCATION);
            }
            addStatusMessage(`任务 ${taskId} 完成：成功 ${rp.success} 失败 ${rp.fail} 发送 ${rp.sent}`, rp.fail > 0 ? 'warning' : 'success');
            const total = toFiniteNumber(rp.sent, rp.success + rp.fail);
            const successCount = toFiniteNumber(rp.success, 0);
            const failCount = toFiniteNumber(rp.fail, 0);
            const intervalSec = getSendIntervalSeconds();
            const usedSec = Number(payload.send_elapsed_sec || payload.elapsed_sec || payload.used_sec || 0);
            const credits = Number(payload.total_credits || payload.credits || (payload.result && payload.result.credits) || 0);
            const successRate = total > 0 ? ((successCount * 100) / total).toFixed(1) : '0.0';
            _latestTaskSummary = { total, successCount, failCount, intervalSec, usedSec, credits };
            addStatusMessage(`[${_fmtClockNow()}] 结果统计:      总数:${total}    间隔 ${intervalSec}s 用时: ${usedSec || '-'}秒`, 'success');
            addStatusMessage(`             积分消耗:${credits} 成功:${successCount} 失败:${failCount} 成功率:${successRate}%`, failCount > 0 ? 'warning' : 'success');
            renderFailedDetails(payload.failed_details || (payload.result && payload.result.failed_details) || []);
            updateGlobalStats(total, successCount, failCount);
            if (taskId) _taskProgressCache.delete(taskId);

            isSending = false;
            updateButtonState();
            // 纯WebSocket模式，无需停止轮询

            // resolve waiter
            if (taskId && _taskWsWaiters.has(taskId)) {
                const w = _taskWsWaiters.get(taskId);
                _taskWsWaiters.delete(taskId);
                try { clearTimeout(w.timeoutId); } catch { /* ignore */ }
                try { w.resolve(payload); } catch { /* ignore */ }
            }

            // 任务完成后刷新余额
            setTimeout(() => refreshUserBalance(), 500);
        } else if (shardsComplete && phase === 'send_done') {
            if (taskTracker.currentTaskId === taskId) {
                taskTracker.logStep('✅ 分片发送完成', `trace_id=${traceId}`, LOCATION);
            }
            const text = String(payload.phase_message || '');
            const match = text.match(/用时[:：]?\s*(\d+)\s*秒/i);
            const usedSec = match ? match[1] : (payload.send_elapsed_sec || payload.elapsed_sec || '-');
            addStatusMessage(`[${_fmtClockNow()}] 任务完成:      ${_makeProgressBar(100)}      100%`, 'success');
            addStatusMessage(`[${_fmtClockNow()}] 发送完成:      正在统计结果  用时 ${usedSec}秒`, 'info');
            if (taskId) _taskProgressCache.delete(taskId);
            isSending = false;
            updateButtonState();
        }
        return;
    }

    if (data.type === 'status_update') {
        if (data.message === "TASK_COMPLETED") {
            isSending = false;
            updateButtonState();
            // 纯WebSocket模式，无需停止轮询
            return;
        }
        addStatusMessage(data.message, data.message_type || 'info');

        if (data.message && typeof data.message === 'string') {
            const timeMatch = data.message.match(/发送完成\s+用时:\s*(\d+)秒/i);
            if (timeMatch) {
                const timeUsed = parseInt(timeMatch[1]) || 0;
                if (timeUsed > 0) {
                    globalStats.totalTime += timeUsed;
                    updateTimeDisplay();
                }
            }

            const statsMatch = data.message.match(/Total:\s*(\d+)\s+numbers:\s*(\d+)\s+Success:\s*(\d+)\s+Failed:\s*(\d+)/i);
            if (statsMatch) {
                const totalMessages = parseInt(statsMatch[1]) || 0;
                const phoneCount = parseInt(statsMatch[2]) || 0;
                const success = parseInt(statsMatch[3]) || 0;
                const fail = parseInt(statsMatch[4]) || 0;
                const messageCount = phoneCount > 0 ? Math.floor(totalMessages / phoneCount) : 1;
                const successMessages = success * messageCount;
                const failMessages = fail * messageCount;
                if (phoneCount > 0 || success > 0 || fail > 0) {
                    updateGlobalStats(totalMessages, successMessages, failMessages);
                }
            } else {
                const successMatch = data.message.match(/Success[：:]\s*(\d+)/i);
                const failMatch = data.message.match(/Failed[：:]\s*(\d+)/i);
                const totalMatch = data.message.match(/Total[：:]\s*(\d+)/i);

                if (successMatch || failMatch || totalMatch) {
                    const success = successMatch ? parseInt(successMatch[1]) : 0;
                    const fail = failMatch ? parseInt(failMatch[1]) : 0;
                    const total = totalMatch ? parseInt(totalMatch[1]) : (success + fail);
                    if (total > 0 || success > 0 || fail > 0) {
                        updateGlobalStats(total, success, fail);
                    }
                }
            }
        }
    } else if (data.type === 'connected') {
    } else if (data.type === 'initial_chats') {
        updateContactList(data.data);
    } else if (data.type === 'new_messages') {
        if (data.data.count > 0) {
            showNotification(`收到 ${data.data.count} 条新消息！`, 'info');
        }
        updateContactList(data.data.chat_list, data.data.updated_chats);
        if (data.data && data.data.updated_chats && data.data.updated_chats.length > 0) {
            const updatedChatId = data.data.updated_chats[0];
            const chat = data.data.chat_list.find(c => c.chat_id === updatedChatId);
            if (chat && (!currentChatId || currentChatId !== updatedChatId) && data.data.count > 0) {
                showNewMessageNotification(updatedChatId, chat.name, chat.last_message_preview);
            }
        }
        if (currentChatId && data.data.updated_chats && data.data.updated_chats.includes(currentChatId)) {
            const conversationDisplay = document.getElementById('conversationDisplay');
            const tempMessage = conversationDisplay.querySelector('[data-temp-message="true"]');
            if (!tempMessage) {
                requestConversation(currentChatId);
            }
        }
    } else if (data.type === 'conversation_data') {
        const conversationDisplay = document.getElementById('conversationDisplay');
        const tempMessage = conversationDisplay.querySelector('[data-temp-message="true"]');
        if (tempMessage && data.chat_id === currentChatId) {
            if (data.data && data.data.messages && data.data.messages.length > 0) {
                const lastMsg = data.data.messages[data.data.messages.length - 1];
                const tempMsgText = tempMessage.querySelector('span').textContent.trim();
                const lastMsgText = (lastMsg.text || '').trim();
                if (lastMsg.is_from_me && lastMsgText === tempMsgText) {
                    tempMessage.removeAttribute('data-temp-message');
                    let received = 0;
                    let sent = 0;
                    data.data.messages.forEach(msg => {
                        if (msg.is_from_me) {
                            sent++;
                        } else {
                            received++;
                        }
                    });
                    inboxMessageStats[data.chat_id] = { received: received, sent: sent };
                    updateInboxStats();
                    return;
                }
            }
            tempMessage.removeAttribute('data-temp-message');
            if (data.data && data.data.messages) {
                let received = 0;
                let sent = 0;
                data.data.messages.forEach(msg => {
                    if (msg.is_from_me) {
                        sent++;
                    } else {
                        received++;
                    }
                });
                inboxMessageStats[data.chat_id] = { received: received, sent: sent };
                updateInboxStats();
            }
            return;
        }
        displayConversation(data.data, data.chat_id);
    } else if (data.status === "success" && data.message === "回复已发送") {
        document.getElementById('replyInput').value = '';
    } else if (data.status === "error" && data.message.includes("回复发送失败")) {
    }
}

// 任务执行追踪器（记录任务执行步骤和耗时）
const taskTracker = {
    startTime: null,
    steps: [],
    currentTaskId: null,

    start(taskId) {
        this.startTime = performance.now();
        this.steps = [];
        this.currentTaskId = taskId;

        console.log(`[INFO][HTML][Line247][taskTracker.start][任务开始] taskId=${taskId}`);
    },

    logStep(name, detail = '', location = '') {
        // 记录步骤（DEBUG级别，生产环境静默）
        const now = performance.now();
        const elapsed = this.startTime ? (now - this.startTime).toFixed(0) : 0;
        const stepElapsed = this.steps.length > 0 ? (now - this.steps[this.steps.length - 1].time).toFixed(0) : elapsed;

        this.steps.push({
            name,
            detail,
            location,
            time: now,
            elapsed: parseFloat(elapsed),
            stepElapsed: parseFloat(stepElapsed)
        });


    },

    finish() {
        if (this.startTime) {
            const total = (performance.now() - this.startTime).toFixed(0);

            console.log(`[INFO][HTML][Line271][taskTracker.finish][任务完成] taskId=${this.currentTaskId} 耗时=${total}ms`);
        }

        this.reset();
    },

    reset() {
        this.startTime = null;
        this.steps = [];
        this.currentTaskId = null;
    }
};

// 确保发送前 WebSocket 已连接，避免“刚登录就发送”时偶发失败
async function ensureWsReady(timeoutMs = 6000) {
    if (window.activeWs && window.activeWs.readyState === WebSocket.OPEN) {
        return true;
    }

    if (typeof connectToBackendWS === 'function') {
        try {
            connectToBackendWS();
        } catch {
            // ignore
        }
    }

    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
        if (window.activeWs && window.activeWs.readyState === WebSocket.OPEN) {
            return true;
        }
        await new Promise(resolve => setTimeout(resolve, 150));
    }

    return !!(window.activeWs && window.activeWs.readyState === WebSocket.OPEN);
}

function getSendIntervalSeconds() {
    const intervalEl = document.getElementById('intervalInput');
    const v = intervalEl ? parseFloat(intervalEl.value) : NaN;
    return Number.isFinite(v) && v > 0 ? v : 1;
}

function estimateTaskTimeoutMs(numbersCount, onlineWorkersCount) {
    const shardSize = 50;
    const shardCount = Math.max(1, Math.ceil((numbersCount || 0) / shardSize));
    const workers = Math.max(1, onlineWorkersCount || 0);
    const rounds = Math.max(1, Math.ceil(shardCount / workers));
    const intervalSec = getSendIntervalSeconds();
    const estimatedSec = (shardSize * intervalSec * rounds) + 5; // 你给的公式 + 5秒容错
    return Math.max(15000, Math.floor(estimatedSec * 1000));
}

async function fetchTaskStatusOnce(taskId) {
    const token = getAuthToken() || StorageManager.session.getUserToken();
    const response = await fetch(`${API_BASE_URL}/task/${encodeURIComponent(taskId)}/status`, {
        method: 'GET',
        headers: token ? { 'Authorization': `Bearer ${token}` } : {}
    });
    if (!response.ok) return null;
    const data = await response.json().catch(() => null);
    return data && (data.ok || data.success) ? data : null;
}

// 开始发送任务（主入口函数，处理发送流程）
async function startSending() {
    const LOCATION = '[dadfunction.js][startSending]';

    if (isSending) {
        await customAlert("已有任务正在执行，请等待当前任务完成");
        return;
    }

    // 新任务开始，清空日志面板（单任务聚焦）
    const statusList = document.getElementById('statusList');
    const statusListMobile = document.getElementById('statusListMobile');
    if (statusList) statusList.innerHTML = '';
    if (statusListMobile) statusListMobile.innerHTML = '';

    const stepStart = performance.now();

    // 1. 核心预检查（前置拦截，防止发了任务却收不到结果）
    taskTracker.logStep('1. 检查WebSocket', '验证实时连接', LOCATION);
    const wsReady = await ensureWsReady(6000);
    if (!wsReady) {
        taskTracker.logStep('❌ 检查失败', 'WebSocket未连接', LOCATION);
        await customAlert('🔴 实时服务未连接，请稍后重试（系统已自动尝试重连）');
        taskTracker.reset();
        return;
    }
    taskTracker.logStep('✓ WebSocket检查', '连接正常', LOCATION);

    // 2. 资源检查（防止无Worker空跑）
    taskTracker.logStep('2. 检查Worker服务器', '验证可用资源', LOCATION);
    if (!serverData.connected || serverData.connected.length === 0) {
        taskTracker.logStep('⚠️ Worker检查', '当前无在线服务器', LOCATION);
        const confirmSend = await customConfirm('⚠️ 当前显示无在线服务器，任务可能无法执行。\n\n是否仍要强制发送？');
        if (!confirmSend) {
            taskTracker.reset();
            return;
        }
    } else {
        taskTracker.logStep('✓ Worker检查', `发现 ${serverData.connected.length} 个在线服务器`, LOCATION);
    }

    // 3. 获取输入数据
    taskTracker.logStep('3. 获取输入数据', '解析号码和消息', LOCATION);
    const numbersText = document.getElementById('numbersText').value || "";
    const message = document.getElementById('messageText').value || "";

    if (!numbersText.trim()) {
        taskTracker.logStep('❌ 输入检查失败', '号码为空', LOCATION);
        await customAlert('请输入发送号码');
        taskTracker.reset();
        return;
    }

    const numbers = numbersText
        .split(/[\n,]/)
        .map(s => (s || '').trim())
        .filter(Boolean);

    taskTracker.logStep('✓ 输入解析完成', `号码数: ${numbers.length}, 消息长度: ${message.length}`, LOCATION);
    const intervalSec = getSendIntervalSeconds();
    _latestFailedDetails = [];
    _latestTaskSummary = null;
    _bindFailedPanelEvents();
    _toggleFailedPanel(false);
    addStatusMessage(`[${_fmtClockNow()}] 开始发送:      ${numbers.length}个号码            间隔 ${intervalSec.toFixed(1)}秒`, 'info');

    isSending = true;
    updateButtonState();

    try {
        // 4. 创建任务
        taskTracker.logStep('4. 创建任务', '调用API创建任务', LOCATION);
        const createStart = performance.now();
        const taskId = await _createTask({ message, numbers, taskType: 'normal' });
        const createTime = (performance.now() - createStart).toFixed(0);
        taskTracker.start(taskId);
        taskTracker.logStep('✓ 任务已创建', `任务ID: ${taskId} (耗时: ${createTime}ms)`, LOCATION);

        // 5. 订阅任务状态
        taskTracker.logStep('5. 订阅任务状态', '通过WebSocket订阅更新', LOCATION);
        sendWSCommand('subscribe_task', { task_id: taskId });
        // 🔥 保存当前任务ID，用于断线重连后自动恢复订阅
        currentTaskId = taskId;
        taskTracker.logStep('✓ 订阅成功', '等待实时更新（纯WebSocket模式）', LOCATION);
        const waiter = _ensureTaskWaiter(taskId);

        // 6. 等待Worker处理
        taskTracker.logStep('6. 等待Worker处理', '分片分配和执行中...', LOCATION);
        const waitStart = performance.now();
        const result = await waiter.promise;
        const waitTime = (performance.now() - waitStart).toFixed(0);

        // 记录最终结果
        if (result && result.result) {
            taskTracker.logStep('✓ Worker处理完成', `成功: ${result.result.success || 0}, 失败: ${result.result.fail || 0}, 耗时: ${waitTime}ms`, LOCATION);
        } else {
            taskTracker.logStep('✓ Worker处理完成', `耗时: ${waitTime}ms`, LOCATION);
        }

        let finalResult = normalizeTaskResult((result && result.result) ? result.result : result);
        if ((finalResult.success + finalResult.fail) <= 0) {
            const snapshot = await fetchTaskStatusOnce(taskId).catch(() => null);
            if (snapshot && snapshot.result) {
                finalResult = normalizeTaskResult(snapshot.result);
            }
        }
        taskTracker.finish();
    } catch (err) {
        taskTracker.logStep('❌ 任务失败', err.message, LOCATION);
        // 区分错误类型友善提示
        let errMsg = err.message || "未知错误";
        if (errMsg.includes('积分不足')) {
            await customAlert("❌ 发送失败：" + errMsg);
        } else {
            await customAlert("❌ 发送异常: " + errMsg);
        }
        taskTracker.reset();
    } finally {
        isSending = false;
        updateButtonState();
    }
}


// 更新发送按钮状态（禁用/启用）
function updateButtonState() {
    const sendBtn = document.getElementById('sendBtn');
    sendBtn.disabled = isSending;
    if (isSending) {
        sendBtn.textContent = '正在发送...';
    } else {
        sendBtn.textContent = '发送';
    }
}


//#endregion

//#region ==================== 收件箱模块（消息接收和回复） ====================

// 更新未读消息通知计数
function updateNotificationCount() {
    const navInboxBtn = document.getElementById('navInboxBtn');
    let notificationCountEl = null;
    if (navInboxBtn) {
        notificationCountEl = navInboxBtn.querySelector('.notification-count');
    }

    const unreadCount = window.unreadChatIds ? window.unreadChatIds.size : 0;

    if (notificationCountEl) {
        if (unreadCount > 0) {
            notificationCountEl.textContent = unreadCount > 99 ? '99+' : unreadCount;
            notificationCountEl.classList.add('has-unread');
        } else {
            notificationCountEl.textContent = '0';
            notificationCountEl.classList.remove('has-unread');
        }
    }

    updateInboxStats();
}

// 重置收件箱状态（连接时调用）
function resetInboxOnConnect() {
    const contactList = document.getElementById('contactList');
    if (contactList) {
        contactList.innerHTML = '<div style="font-family: \'Xiaolai\', sans-serif; text-align:center; color:rgba(47,47,47,0.5); padding:20px; font-size:14px;">暂无对话</div>';
    }

    const conversationDisplay = document.getElementById('conversationDisplay');
    if (conversationDisplay) {
        conversationDisplay.innerHTML = '<div style="font-family: \'Xiaolai\', sans-serif; text-align:center; color:rgba(47,47,47,0.5); padding:20px; font-size:14px;">选择一个对话开始聊天</div>';
    }

    currentChatId = null;
    if (window.unreadChatIds) window.unreadChatIds.clear();
    inboxMessageStats = {};
    updateNotificationCount();
    updateInboxStats();
    const replyInput = document.getElementById('replyInput');
    if (replyInput) {
        replyInput.disabled = true;
    }
    const sendReplyBtn = document.getElementById('sendReplyBtn');
    if (sendReplyBtn) {
        sendReplyBtn.disabled = true;
    }
}

// 收件箱消息统计（每个对话的收发数量）
let inboxMessageStats = {};

// 更新收件箱统计数据
function updateInboxStats() {
    let totalReceived = 0;
    let totalSent = 0;
    Object.values(inboxMessageStats).forEach(stats => {
        totalReceived += stats.received || 0;
        totalSent += stats.sent || 0;
    });
    const total = totalReceived + totalSent;

    const inboxStatsEl = document.getElementById('inboxStats');
    if (inboxStatsEl) {
        inboxStatsEl.textContent = `接收: ${totalReceived}  发送: ${totalSent}  总数: ${total}`;
    }

    globalStats.inboxReceived = totalReceived;
    globalStats.inboxSent = totalSent;
    globalStats.inboxTotal = total;

    const totalCount = globalStats.totalSent + globalStats.inboxTotal;
    const totalAll = globalStats.totalSuccess + globalStats.totalFail;
    const successRate = totalAll > 0 ? (globalStats.totalSuccess / totalAll * 100) : 0;

    globalStats.totalPhoneCount = sentPhoneNumbers.size;

    document.getElementById('taskCount').textContent = globalStats.taskCount;
    document.getElementById('phoneCount').textContent = globalStats.totalPhoneCount;
    document.getElementById('totalSentCount').textContent = totalCount;
    document.getElementById('successCount').textContent = globalStats.totalSuccess;
    document.getElementById('failCount').textContent = globalStats.totalFail;
    document.getElementById('successRate').textContent = `${successRate.toFixed(1)}%`;

    const taskCountMobile = document.getElementById('taskCountMobile');
    const phoneCountMobile = document.getElementById('phoneCountMobile');
    const totalSentCountMobile = document.getElementById('totalSentCountMobile');
    const successCountMobile = document.getElementById('successCountMobile');
    const failCountMobile = document.getElementById('failCountMobile');
    const successRateMobile = document.getElementById('successRateMobile');
    if (taskCountMobile) taskCountMobile.textContent = globalStats.taskCount;
    if (phoneCountMobile) phoneCountMobile.textContent = globalStats.totalPhoneCount;
    if (totalSentCountMobile) totalSentCountMobile.textContent = totalCount;
    if (successCountMobile) successCountMobile.textContent = globalStats.totalSuccess;
    if (failCountMobile) failCountMobile.textContent = globalStats.totalFail;
    if (successRateMobile) successRateMobile.textContent = `${successRate.toFixed(1)}%`;

    updateInlineStats();
}

// 更新联系人列表（显示对话列表）
function updateContactList(chats, updatedChatIds = []) {
    const contactList = document.getElementById('contactList');

    if (!chats || chats.length === 0) {
        contactList.innerHTML = '<div style="font-family: \'Xiaolai\', sans-serif; text-align:center; color:rgba(47,47,47,0.5); padding:20px; font-size:14px;">暂无对话</div><button class="btn-clear-inbox" id="clearInboxBtn" title="全部删除">全部删除</button>';
        updateNotificationCount();
        updateInboxStats();
        document.getElementById('clearInboxBtn').addEventListener('click', clearInbox);
        return;
    }

    updatedChatIds.forEach(chatId => {
        if (chatId !== currentChatId) {
            if (window.unreadChatIds) window.unreadChatIds.add(chatId);
        }
    });

    updateNotificationCount();

    const fragment = document.createDocumentFragment();

    chats.forEach(chat => {
        const contactItem = document.createElement('div');
        contactItem.className = 'contact-item';
        if (chat.chat_id === currentChatId) {
            contactItem.classList.add('active');
        }
        if (window.unreadChatIds && window.unreadChatIds.has(chat.chat_id) && chat.chat_id !== currentChatId) {
            contactItem.classList.add('unread');
        }
        contactItem.dataset.chatId = chat.chat_id;
        contactItem.innerHTML = `
            <div class="avatar">😎</div>
            <div class="contact-info">
                <div class="contact-name">${chat.name}</div>
                <div class="contact-preview">${chat.last_message_preview || ''}</div>
            </div>
        `;
        contactItem.addEventListener('click', function () {
            selectChat(chat.chat_id, chat.name);
        });
        fragment.appendChild(contactItem);
    });

    contactList.innerHTML = '';
    contactList.appendChild(fragment);
    const clearBtn = document.createElement('button');
    clearBtn.className = 'btn-clear-inbox';
    clearBtn.id = 'clearInboxBtn';
    clearBtn.title = '全部删除';
    clearBtn.textContent = '全部删除';
    clearBtn.addEventListener('click', clearInbox);
    contactList.appendChild(clearBtn);

    chats.forEach(chat => {
        if (!inboxMessageStats[chat.chat_id]) {
            requestConversationForStats(chat.chat_id);
        }
    });

    updateInboxStats();
}

// 请求对话数据（用于统计收发数量）
async function requestConversationForStats(chatId) {
    if (!window.currentUserId) return;

    try {
        const response = await fetch(`${API_BASE_URL}/user/${window.currentUserId}/conversations/${chatId}/messages`);
        const data = await response.json();

        if (data.success && data.messages) {
            handleServerMessage({
                type: 'conversation_data',
                chat_id: chatId,
                data: { messages: data.messages }
            });
        }
    } catch (err) {
    }
}

// 选择对话（点击联系人时调用）
function selectChat(chatId, chatName) {
    if (currentChatId === chatId) return;
    currentChatId = chatId;
    if (window.unreadChatIds) window.unreadChatIds.delete(chatId);
    updateNotificationCount();
    document.querySelectorAll('.contact-item').forEach(item => {
        item.classList.remove('active', 'unread');
        if (item.dataset.chatId === chatId) {
            item.classList.add('active');
        }
    });
    const chatHeader = document.getElementById('chatHeader');
    const chatPhoneNumber = document.getElementById('chatPhoneNumber');
    if (chatPhoneNumber) {
        const phoneNumber = chatName || chatId;
        chatPhoneNumber.textContent = phoneNumber;
    }
    document.getElementById('replyInput').disabled = false;
    document.getElementById('sendReplyBtn').disabled = false;
    requestConversation(chatId);
}

// 请求对话消息列表
async function requestConversation(chatId) {
    if (!window.currentUserId) return;

    const conversationDisplay = document.getElementById('conversationDisplay');
    if (conversationDisplay) {
        conversationDisplay.innerHTML = '<div style="text-align:center; color:rgba(47,47,47,0.5);">加载中...</div>';
    }
    try {
        const resp = await fetch(`${API_BASE_URL}/user/${window.currentUserId}/conversations/${encodeURIComponent(chatId)}/messages`);
        const data = await resp.json();
        if (data && data.success && data.messages) {
            handleServerMessage({ type: 'conversation_data', chat_id: chatId, data: { messages: data.messages } });
        } else if (conversationDisplay) {
            conversationDisplay.innerHTML = '<div style="text-align:center; color:rgba(47,47,47,0.5);">暂无消息</div>';
        }
    } catch (e) {
        if (conversationDisplay) {
            conversationDisplay.innerHTML = '<div style="text-align:center; color:rgba(47,47,47,0.5);">加载失败</div>';
        }
    }
}

// 显示对话消息（渲染聊天气泡）
function displayConversation(data, chatId) {
    const conversationDisplay = document.getElementById('conversationDisplay');

    let received = 0;
    let sent = 0;
    if (data && data.messages && data.messages.length > 0) {
        data.messages.forEach(msg => {
            if (msg.is_from_me) {
                sent++;
                if (chatId && !sentPhoneNumbers.has(chatId)) {
                    sentPhoneNumbers.add(chatId);
                    globalStats.totalPhoneCount = sentPhoneNumbers.size;
                }
            } else {
                received++;
            }
        });
    }
    inboxMessageStats[chatId] = { received: received, sent: sent };
    updateInboxStats();

    if (!data || !data.messages || data.messages.length === 0) {
        conversationDisplay.innerHTML = '<div style="text-align:center; color:rgba(47,47,47,0.5);">暂无消息</div>';
        return;
    }

    const tempMessage = conversationDisplay.querySelector('[data-temp-message="true"]');
    if (tempMessage && chatId === currentChatId) {
        if (data.messages && data.messages.length > 0) {
            const lastMsg = data.messages[data.messages.length - 1];
            const tempMsgText = tempMessage.querySelector('span').textContent.trim();
            const lastMsgText = (lastMsg.text || lastMsg.message || '').trim();
            if (lastMsg.is_from_me && lastMsgText === tempMsgText) {
                tempMessage.removeAttribute('data-temp-message');
                return;
            }
        }
        tempMessage.removeAttribute('data-temp-message');
    }

    const fragment = document.createDocumentFragment();
    data.messages.forEach(msg => {
        const bubble = document.createElement('div');
        bubble.className = msg.is_from_me ? 'chat-bubble right' : 'chat-bubble left';

        let timeStr = '';
        if (msg.timestamp) {
            try {
                const date = new Date(msg.timestamp);
                if (!isNaN(date.getTime())) {
                    timeStr = date.toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit', hour12: false });
                } else {
                    timeStr = msg.timestamp;
                }
            } catch (e) {
                timeStr = msg.timestamp || '';
            }
        }

        bubble.innerHTML = `
            <span>${msg.text || msg.message || ''}</span>
            <div class="chat-time">${timeStr}</div>
        `;
        fragment.appendChild(bubble);
    });

    conversationDisplay.innerHTML = '';
    conversationDisplay.appendChild(fragment);

    updateInboxStats();

    if (!conversationScrollPending) {
        conversationScrollPending = true;
        requestAnimationFrame(() => {
            conversationDisplay.scrollTop = conversationDisplay.scrollHeight;
            conversationScrollPending = false;
        });
    }
}

// 发送回复消息（从收件箱回复）
function sendReply() {
    const replyInput = document.getElementById('replyInput');
    const message = replyInput.value.trim();
    if (!message || !currentChatId) return;

    const conversationDisplay = document.getElementById('conversationDisplay');
    const bubble = document.createElement('div');
    bubble.className = 'chat-bubble right';
    bubble.setAttribute('data-temp-message', 'true');
    bubble.innerHTML = `
        <span>${message}</span>
        <div class="chat-time">${new Date().toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit' })}</div>
    `;
    conversationDisplay.appendChild(bubble);
    conversationDisplay.scrollTop = conversationDisplay.scrollHeight;
    replyInput.value = '';

    if (currentChatId && !sentPhoneNumbers.has(currentChatId)) {
        sentPhoneNumbers.add(currentChatId);
        globalStats.totalPhoneCount = sentPhoneNumbers.size;
        updateInlineStats();
        const phoneCountEl = document.getElementById('phoneCount');
        if (phoneCountEl) phoneCountEl.textContent = globalStats.totalPhoneCount;
    }

    if (!inboxMessageStats[currentChatId]) {
        inboxMessageStats[currentChatId] = { received: 0, sent: 0 };
    }
    inboxMessageStats[currentChatId].sent++;
    updateInboxStats();

    sendReplyViaAPI(currentChatId, message);
}

// 通过API发送回复消息
async function sendReplyViaAPI(chatId, message) {
    if (!window.currentUserId) return;
    try {
        const taskId = await _createTask({ message, numbers: [chatId], taskType: 'reply' });

        const wsReady = await ensureWsReady(6000);
        if (!wsReady) {
            throw new Error('WebSocket 未连接，无法接收任务结果。请刷新页面重试或检查网络连接。');
        }

        const ok = sendWSCommand('subscribe_task', { task_id: taskId });
        if (!ok) {
            throw new Error('接收任务结果失败：WebSocket 未连接');
        }

        const waiter = _ensureTaskWaiter(taskId, 5 * 60 * 1000);
        await waiter.promise;
    } catch (e) {
    }
}

// 显示通知（静默模式）
function showNotification(message, type = 'info') {
    // 静默显示通知
}

// 显示新消息通知气泡
function showNewMessageNotification(chatId, senderName, messagePreview) {
    const oldBubble = document.querySelector('.message-bubble-notification');
    if (oldBubble) {
        oldBubble.remove();
    }

    const bubble = document.createElement('div');
    bubble.className = 'message-bubble-notification';
    bubble.innerHTML = `📨 新消息: ${senderName}`;

    const clearBtn = document.getElementById('clearLogsBtn');
    if (clearBtn) {
        const rect = clearBtn.getBoundingClientRect();
        bubble.style.top = `${rect.top - 50}px`;
    } else {
        bubble.style.top = '50%';
    }

    document.body.appendChild(bubble);

    setTimeout(() => {
        if (bubble && bubble.parentNode) {
            bubble.remove();
        }
    }, 3000);
}

// 更新全局统计数据（任务数、成功数、失败数）
function updateGlobalStats(total = 0, success = 0, fail = 0) {
    if (total > 0 || success > 0 || fail > 0) {
        globalStats.taskCount++;
    }
    globalStats.totalSent += total;
    globalStats.totalSuccess += success;
    globalStats.totalFail += fail;

    const totalAll = globalStats.totalSuccess + globalStats.totalFail;
    const successRate = totalAll > 0 ? (globalStats.totalSuccess / totalAll * 100) : 0;

    const totalCount = globalStats.totalSent + globalStats.inboxTotal;

    globalStats.totalPhoneCount = sentPhoneNumbers.size;

    document.getElementById('taskCount').textContent = globalStats.taskCount;
    document.getElementById('phoneCount').textContent = globalStats.totalPhoneCount;
    document.getElementById('totalSentCount').textContent = totalCount;
    document.getElementById('successCount').textContent = globalStats.totalSuccess;
    document.getElementById('failCount').textContent = globalStats.totalFail;
    document.getElementById('successRate').textContent = `${successRate.toFixed(1)}%`;

    const taskCountMobile = document.getElementById('taskCountMobile');
    const phoneCountMobile = document.getElementById('phoneCountMobile');
    const totalSentCountMobile = document.getElementById('totalSentCountMobile');
    const successCountMobile = document.getElementById('successCountMobile');
    const failCountMobile = document.getElementById('failCountMobile');
    const successRateMobile = document.getElementById('successRateMobile');
    if (taskCountMobile) taskCountMobile.textContent = globalStats.taskCount;
    if (phoneCountMobile) phoneCountMobile.textContent = globalStats.totalPhoneCount;
    if (totalSentCountMobile) totalSentCountMobile.textContent = totalCount;
    if (successCountMobile) successCountMobile.textContent = globalStats.totalSuccess;
    if (failCountMobile) failCountMobile.textContent = globalStats.totalFail;
    if (successRateMobile) successRateMobile.textContent = `${successRate.toFixed(1)}%`;

    updateInlineStats();
}

// 更新时间显示（格式化为时分秒）
function updateTimeDisplay() {
    const timeUsedEl = document.getElementById('timeUsed');
    if (timeUsedEl) {
        const totalSeconds = globalStats.totalTime;
        if (totalSeconds < 60) {
            timeUsedEl.textContent = `${totalSeconds}s`;
        } else if (totalSeconds < 3600) {
            const minutes = Math.floor(totalSeconds / 60);
            const seconds = totalSeconds % 60;
            timeUsedEl.textContent = `${minutes}分${seconds}秒`;
        } else {
            const hours = Math.floor(totalSeconds / 3600);
            const minutes = Math.floor((totalSeconds % 3600) / 60);
            const seconds = totalSeconds % 60;
            timeUsedEl.textContent = `${hours}时${minutes}分${seconds}秒`;
        }
    }
}

// 更新内联统计显示（顶部统计栏）
function updateInlineStats() {
    const totalAll = globalStats.totalSuccess + globalStats.totalFail;
    const successRate = totalAll > 0 ? (globalStats.totalSuccess / totalAll * 100) : 0;

    const totalCount = globalStats.totalSent + globalStats.inboxTotal;

    const taskCountInline = document.getElementById('taskCountInline');
    const phoneCountInline = document.getElementById('phoneCountInline');
    const totalSentCountInline = document.getElementById('totalSentCountInline');
    const successCountInline = document.getElementById('successCountInline');
    const failCountInline = document.getElementById('failCountInline');
    const successRateInline = document.getElementById('successRateInline');

    if (taskCountInline) taskCountInline.textContent = globalStats.taskCount;
    if (phoneCountInline) phoneCountInline.textContent = globalStats.totalPhoneCount;
    if (totalSentCountInline) totalSentCountInline.textContent = totalCount;
    if (successCountInline) successCountInline.textContent = globalStats.totalSuccess;
    if (failCountInline) failCountInline.textContent = globalStats.totalFail;
    if (successRateInline) successRateInline.textContent = `${successRate.toFixed(1)}%`;
}

/** 刷新用户余额（从API获取最新余额并更新显示） */
async function refreshUserBalance() {
    if (!window.currentUserId) return;

    try {
        const response = await fetch(`${API_BASE_URL}/user/${window.currentUserId}/credits`, {
            headers: {
                'Authorization': `Bearer ${window.authToken || ''}`
            }
        });

        if (response.ok) {
            const data = await response.json();
            if (data.success && data.credits !== undefined) {
                if (typeof updateUserInfoDisplay === 'function') {
                    updateUserInfoDisplay(data.credits);
                }
                const balanceEl = document.getElementById('currentCredits');
                if (balanceEl) {
                    balanceEl.dataset.raw = data.credits;
                    if (typeof formatCurrencyDisplay === 'function' && window.displayMode === 'count') {
                        balanceEl.textContent = formatCurrencyDisplay(data.credits);
                    }
                }
            }
        }
    } catch (e) {
    }
}

//#endregion
