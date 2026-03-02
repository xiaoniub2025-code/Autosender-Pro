// 生产环境静默控制台输出（正式环境去除调试信息）

//#region ==================== 全局变量初始化 ====================
// 确保全局变量存在（由12admin_page.js定义）
if (typeof globalStats === 'undefined') window.globalStats = { taskCount: 0, totalSent: 0, totalSuccess: 0, totalFail: 0, totalTime: 0, totalPhoneCount: 0, inboxReceived: 0, inboxSent: 0, inboxTotal: 0 };
if (typeof sentPhoneNumbers === 'undefined') window.sentPhoneNumbers = new Set();
//#endregion

//#region ==================== 统计显示模块 ====================

// 初始化全局统计数据显示（任务数、号码数、成功率等）
function initGlobalStatsDisplay() {
    globalStats.totalPhoneCount = sentPhoneNumbers.size;

    const totalCount = globalStats.totalSent + globalStats.inboxTotal;
    document.getElementById('taskCount').textContent = globalStats.taskCount;
    document.getElementById('phoneCount').textContent = globalStats.totalPhoneCount;
    document.getElementById('totalSentCount').textContent = totalCount;
    document.getElementById('successCount').textContent = globalStats.totalSuccess;
    document.getElementById('failCount').textContent = globalStats.totalFail;
    const totalAll = globalStats.totalSuccess + globalStats.totalFail;
    const successRate = totalAll > 0 ? (globalStats.totalSuccess / totalAll * 100) : 0;
    document.getElementById('successRate').textContent = `${successRate.toFixed(1)}%`;
    updateTimeDisplay();
    updateInlineStats();
}
//#endregion

//#region ==================== 面板切换模块 ====================

// 切换显示面板（主页、发送、收件箱、账号管理）
function switchPanel(panelType) {
    const workspaceAB = document.getElementById('workspaceAB');
    const panelB = document.getElementById('panelB');
    const panelC = document.getElementById('panelC');
    const panelD = document.getElementById('panelD');
    const panelE = document.getElementById('panelE');
    const navHomeBtn = document.getElementById('navHomeBtn');
    const navAccountBtn = document.getElementById('navAccountBtn');
    const navSendBtn = document.getElementById('navSendBtn');
    const navInboxBtn = document.getElementById('navInboxBtn');
    const logPanelBtn = document.getElementById('logPanelBtn');

    if (navHomeBtn) navHomeBtn.classList.remove('active');
    if (navAccountBtn) navAccountBtn.classList.remove('active');
    if (navSendBtn) navSendBtn.classList.remove('active');
    if (navInboxBtn) navInboxBtn.classList.remove('active');

    const isMobile = window.innerWidth <= 768;

    if (workspaceAB) {
        workspaceAB.style.display = 'none';
        workspaceAB.classList.remove('mobile-show', 'single-panel', 'show-log');
    }
    if (panelB) {
        panelB.classList.remove('mobile-show');
        panelB.style.display = 'none';
    }
    if (panelC) {
        panelC.classList.remove('mobile-show');
        panelC.style.display = 'none';
    }
    if (panelD) {
        panelD.classList.remove('mobile-show');
        panelD.style.display = 'none';
    }
    if (panelE) {
        panelE.classList.remove('mobile-show');
        panelE.style.display = 'none';
    }
    if (logPanelBtn) logPanelBtn.classList.remove('active');

    if (panelType === 'home') {
        if (isMobile) {
            if (panelD) {
                panelD.classList.add('mobile-show');
                panelD.style.display = 'flex';
            }
        } else {
            if (panelD) panelD.style.display = 'flex';
        }
        if (navHomeBtn) navHomeBtn.classList.add('active');
    } else if (panelType === 'send') {
        if (isMobile) {
            if (workspaceAB) {
                workspaceAB.classList.add('mobile-show', 'single-panel');
                workspaceAB.style.display = 'flex';
                workspaceAB.classList.remove('show-log');
            }
        } else {
            if (workspaceAB) {
                workspaceAB.style.display = 'flex';
                workspaceAB.classList.add('single-panel');
                workspaceAB.classList.remove('show-log');
            }
        }
        if (navSendBtn) navSendBtn.classList.add('active');
    } else if (panelType === 'inbox') {
        if (isMobile) {
            if (panelC) {
                panelC.classList.add('mobile-show');
                panelC.style.display = 'flex';
            }
        } else {
            if (panelC) panelC.style.display = 'flex';
        }
        if (navInboxBtn) navInboxBtn.classList.add('active');
    } else if (panelType === 'account') {
        if (isMobile) {
            if (panelE) {
                panelE.classList.add('mobile-show');
                panelE.style.display = 'flex';
            }
        } else {
            if (panelE) panelE.style.display = 'flex';
        }
        if (navAccountBtn) navAccountBtn.classList.add('active');
        
        // 延迟加载账号管理内容，确保用户ID已设置
        setTimeout(() => {
            if (typeof loadAccountPanelContent === 'function') {
                loadAccountPanelContent();
            }
        }, 100);
    }
}

// 切换日志面板显示（桌面端展开/收起，移动端全屏）
function toggleLogPanel() {
    const workspaceAB = document.getElementById('workspaceAB');
    const panelB = document.getElementById('panelB');
    const panelC = document.getElementById('panelC');
    const panelD = document.getElementById('panelD');
    const logPanelBtn = document.getElementById('logPanelBtn');

    const isMobile = window.innerWidth <= 768;

    if (isMobile) {
        syncLogToMobile();

        if (workspaceAB) {
            workspaceAB.classList.remove('mobile-show');
            workspaceAB.style.display = 'none';
        }
        if (panelC) {
            panelC.classList.remove('mobile-show');
            panelC.style.display = 'none';
        }
        if (panelD) {
            panelD.classList.remove('mobile-show');
            panelD.style.display = 'none';
        }
        if (panelB) {
            panelB.classList.add('mobile-show');
            panelB.style.display = 'flex';
        }
        if (logPanelBtn) logPanelBtn.classList.add('active');
    } else {
        if (workspaceAB && logPanelBtn) {
            if (workspaceAB.classList.contains('show-log')) {
                workspaceAB.classList.remove('show-log');
                workspaceAB.classList.add('single-panel');
                logPanelBtn.classList.remove('active');
            } else {
                workspaceAB.classList.remove('single-panel');
                workspaceAB.classList.add('show-log');
                logPanelBtn.classList.add('active');
            }
        }
    }
}

// 同步日志内容到移动端显示
function syncLogToMobile() {
    const statusList = document.getElementById('statusList');
    const statusListMobile = document.getElementById('statusListMobile');
    if (statusList && statusListMobile) {
        statusListMobile.innerHTML = statusList.innerHTML;
        statusListMobile.scrollTop = statusListMobile.scrollHeight;
    }

    const statIds = ['taskCount', 'phoneCount', 'totalSentCount', 'successCount', 'failCount', 'successRate'];
    statIds.forEach(id => {
        const source = document.getElementById(id);
        const target = document.getElementById(id + 'Mobile');
        if (source && target) {
            target.textContent = source.textContent;
        }
    });
}

// 返回发送面板（移动端从日志面板返回）
function backToSendPanel() {
    const workspaceAB = document.getElementById('workspaceAB');
    const panelB = document.getElementById('panelB');
    const panelC = document.getElementById('panelC');
    const panelD = document.getElementById('panelD');
    const logPanelBtn = document.getElementById('logPanelBtn');
    const navSendBtn = document.getElementById('navSendBtn');

    const isMobile = window.innerWidth <= 768;

    if (isMobile) {
        if (panelB) {
            panelB.classList.remove('mobile-show');
            panelB.style.display = 'none';
        }
        if (panelC) {
            panelC.classList.remove('mobile-show');
            panelC.style.display = 'none';
        }
        if (panelD) {
            panelD.classList.remove('mobile-show');
            panelD.style.display = 'none';
        }
        if (workspaceAB) {
            workspaceAB.classList.add('mobile-show', 'single-panel');
            workspaceAB.classList.remove('show-log');
            workspaceAB.style.display = 'flex';
        }
        if (logPanelBtn) logPanelBtn.classList.remove('active');
        document.querySelectorAll('.nav-btn').forEach(btn => btn.classList.remove('active'));
        if (navSendBtn) navSendBtn.classList.add('active');
    }
}

// 清空收件箱（删除所有对话）
async function clearInbox() {
    const allChatIds = Array.from(document.querySelectorAll('.contact-item')).map(item => item.dataset.chatId).filter(id => id);

    const contactList = document.getElementById('contactList');
    contactList.innerHTML = '<div style="font-family: \'Xiaolai\', sans-serif; text-align:center; color:rgba(47,47,47,0.5); padding:20px; font-size:14px;">暂无对话</div><button class="btn-clear-inbox" id="clearInboxBtn" title="全部删除">全部删除</button>';
    document.getElementById('conversationDisplay').innerHTML = '<div style="font-family: \'Xiaolai\', sans-serif; text-align:center; color:rgba(47,47,47,0.5); padding:20px; font-size:14px;">选择一个对话开始聊天</div>';
    document.getElementById('chatHeader').innerHTML = ' 选择一个对话';

    allChatIds.forEach(chatId => {
        clearedChatIds.add(chatId);
    });

    inboxMessageStats = {};
    currentChatId = null;
    unreadChatIds.clear();
    updateInboxStats();
    updateNotificationCount();

    if (allChatIds.length > 0 && window.currentUserId) {
        try {
            await Promise.all(allChatIds.map(chatId =>
                fetch(`${API_BASE_URL}/user/${window.currentUserId}/conversations/${encodeURIComponent(chatId)}`, {
                    method: 'DELETE'
                }).catch(() => null)
            ));
        } catch { /* ignore */ }
    }

    document.getElementById('clearInboxBtn').addEventListener('click', clearInbox);
}

//#region ===== 导航按钮事件绑定 =====
document.getElementById('navHomeBtn').addEventListener('click', function () {
    switchPanel('home');
});

document.getElementById('navSendBtn').addEventListener('click', function () {
    switchPanel('send');
});

document.getElementById('navInboxBtn').addEventListener('click', function () {
    switchPanel('inbox');
});

document.getElementById('navAccountBtn').addEventListener('click', function () {
    switchPanel('account');
});

document.getElementById('logPanelBtn').addEventListener('click', function () {
    toggleLogPanel();
});

document.getElementById('backToSendBtn').addEventListener('click', function () {
    backToSendPanel();
});

document.getElementById('clearLogsBtn').addEventListener('click', function () {
    document.getElementById('statusList').innerHTML = '';
    const statusListMobile = document.getElementById('statusListMobile');
    if (statusListMobile) statusListMobile.innerHTML = '';
    const failedDetailsList = document.getElementById('failedDetailsList');
    const failedDetailsListMobile = document.getElementById('failedDetailsListMobile');
    if (failedDetailsList) failedDetailsList.textContent = '';
    if (failedDetailsListMobile) failedDetailsListMobile.textContent = '';
});

document.getElementById('clearLogsBtnMobile').addEventListener('click', function () {
    document.getElementById('statusListMobile').innerHTML = '';
    const statusList = document.getElementById('statusList');
    if (statusList) statusList.innerHTML = '';
    const failedDetailsList = document.getElementById('failedDetailsList');
    const failedDetailsListMobile = document.getElementById('failedDetailsListMobile');
    if (failedDetailsList) failedDetailsList.textContent = '';
    if (failedDetailsListMobile) failedDetailsListMobile.textContent = '';
});

document.getElementById('clearInboxBtn').addEventListener('click', clearInbox);
//#endregion
//#endregion

//#region ==================== 输入辅助功能模块 ====================

// 计算字符串长度（中文字符算2个字符）
function getStringLength(str) {
    let length = 0;
    for (let i = 0; i < str.length; i++) {
        const charCode = str.charCodeAt(i);
        if (charCode >= 0x4E00 && charCode <= 0x9FFF) {
            length += 2;
        } else {
            length += 1;
        }
    }
    return length;
}

// 更新号码和消息计数显示
function updateCounts() {
    const numbersText = document.getElementById('numbersText');
    if (!numbersText) return;
    
    const numbers = numbersText.value.split(/[\n,]/).filter(n => n.trim()).length;
    const numbersCountEl = document.getElementById('numbersCount');

    if (numbersCountEl) {
        if (numbers === 0) {
            numbersCountEl.textContent = `号码: ${numbers}`;
            numbersCountEl.classList.remove('has-numbers');
        } else {
            numbersCountEl.textContent = `号码: ${numbers}`;
            numbersCountEl.classList.add('has-numbers');
        }
    }

    const messageText = document.getElementById('messageText');
    if (!messageText) return;
    
    const charCount = getStringLength(messageText.value);
    const messageCountEl = document.getElementById('messageCount');

    if (messageCountEl) {
        if (charCount === 0) {
            messageCountEl.textContent = `字数: ${charCount}`;
            messageCountEl.classList.remove('has-content', 'over-limit');
        } else if (charCount <= 160) {
            messageCountEl.textContent = `字数: ${charCount}`;
            messageCountEl.classList.remove('over-limit');
            messageCountEl.classList.add('has-content');
        } else {
            const messageCount = Math.ceil(charCount / 160);
            messageCountEl.innerHTML = `字数: ${charCount}/160 <span class="message-count-badge">${messageCount}条</span>`;
            messageCountEl.classList.remove('has-content');
            messageCountEl.classList.add('over-limit');
        }
    }
}

// 导入号码文件（触发文件选择）
function importNumbers() {
    const fileInput = document.getElementById('numbersFile');
    if (fileInput) {
        fileInput.click();
    }
}

// 导入消息文件（触发文件选择）
function importMessage() {
    const fileInput = document.getElementById('messageFile');
    if (fileInput) {
        fileInput.click();
    }
}

// 清空号码输入框
function clearNumbers() {
    const btn = document.getElementById('clearNumbersBtn');
    const numbersText = document.getElementById('numbersText');
    if (numbersText) {
        numbersText.value = '';
        updateCounts();
    }
    if (btn) {
        btn.blur();
        btn.style.background = '';
        btn.style.borderColor = '';
        btn.style.transform = '';
        btn.style.boxShadow = '';
        setTimeout(() => {
            btn.blur();
            btn.style.background = '';
            btn.style.borderColor = '';
        }, 100);
    }
}

// 清空消息输入框
function clearMessage() {
    const btn = document.getElementById('clearMessageBtn');
    const messageText = document.getElementById('messageText');
    if (messageText) {
        messageText.value = '';
        updateCounts();
    }
    if (btn) {
        btn.blur();
        btn.style.background = '';
        btn.style.borderColor = '';
        btn.style.transform = '';
        btn.style.boxShadow = '';
        setTimeout(() => {
            btn.blur();
            btn.style.background = '';
            btn.style.borderColor = '';
        }, 100);
    }
}

//#region ===== 文件输入处理 =====
// 处理号码文件选择
const numbersFileInput = document.getElementById('numbersFile');
if (numbersFileInput) {
    numbersFileInput.addEventListener('change', function (e) {
        const file = e.target.files[0];
        if (file) {
            const reader = new FileReader();
            reader.onload = function (e) {
                const content = e.target.result;
                const numbersText = document.getElementById('numbersText');
                if (numbersText) {
                    numbersText.value = content.trim();
                    updateCounts();
                }
            };
            reader.readAsText(file);
        }
        e.target.value = '';
    });
}

// 处理消息文件选择
const messageFileInput = document.getElementById('messageFile');
if (messageFileInput) {
    messageFileInput.addEventListener('change', function (e) {
        const file = e.target.files[0];
        if (file) {
            const reader = new FileReader();
            reader.onload = function (e) {
                const content = e.target.result;
                const messageText = document.getElementById('messageText');
                if (messageText) {
                    messageText.value = content.trim();
                    updateCounts();
                }
            };
            reader.readAsText(file);
        }
        e.target.value = '';
    });
}
//#endregion

//#region ===== 输入框事件绑定 =====
document.getElementById('numbersText').addEventListener('input', updateCounts);
document.getElementById('messageText').addEventListener('input', updateCounts);
document.getElementById('importNumbersBtn').addEventListener('click', importNumbers);
document.getElementById('clearNumbersBtn').addEventListener('click', clearNumbers);
document.getElementById('importMessageBtn').addEventListener('click', importMessage);
document.getElementById('clearMessageBtn').addEventListener('click', clearMessage);
document.getElementById('sendBtn').addEventListener('click', startSending);
document.getElementById('sendReplyBtn').addEventListener('click', sendReply);
document.getElementById('replyInput').addEventListener('keydown', (e) => e.key === 'Enter' && !e.shiftKey && (e.preventDefault(), sendReply()));
//#endregion
//#endregion

//#region ==================== 界面缩放与调试模块 ====================

// 展开锁定状态
const lockState = {};

// 设置展开/折叠功能（号码和日志区域）
function setupExpand(triggerId, wrapperId) {
    const trigger = document.getElementById(triggerId);
    const wrapper = document.getElementById(wrapperId);
    if (!trigger || !wrapper) return;

    trigger.addEventListener('click', (e) => {
        e.stopPropagation();
        if (lockState[wrapperId]) {
            lockState[wrapperId] = false;
            wrapper.classList.remove('locked');
            wrapper.classList.remove('expanded');
        } else {
            lockState[wrapperId] = true;
            wrapper.classList.add('locked');
            wrapper.classList.add('expanded');
        }
    });
}

// 更新发送间隔颜色指示
function updateIntervalColor() {
    const intervalSelect = document.getElementById('intervalInput');
    if (intervalSelect) {
        const value = intervalSelect.value;
        intervalSelect.setAttribute('data-value', value);
    }
}

// 更新页面缩放比例（响应式适配）
function updateScale() {
    const isMobile = window.innerWidth <= 768;
    if (isMobile) {
        document.body.style.transform = '';
        document.body.style.transformOrigin = '';
        document.body.style.zoom = '';
        return;
    }

    const devicePixelRatio = window.devicePixelRatio || 1;

    const designWidth = 1450;
    const designHeight = 580;

    const minMargin = 50;

    const minWindowWidth = designWidth + (minMargin * 2);
    const minWindowHeight = designHeight + (minMargin * 2);

    const windowWidth = window.innerWidth;
    const windowHeight = window.innerHeight;

    const availableWidth = windowWidth - (minMargin * 2);
    const availableHeight = windowHeight - (minMargin * 2);

    let scale = 1.0;

    if (windowWidth < minWindowWidth || windowHeight < minWindowHeight) {
        const scaleX = availableWidth / designWidth;
        const scaleY = availableHeight / designHeight;
        scale = Math.min(scaleX, scaleY);
        scale = Math.max(0.5, scale);
    }

    const contentWrapper = document.querySelector('.content-wrapper');
    if (contentWrapper) {
        contentWrapper.style.setProperty('width', designWidth + 'px', 'important');
        contentWrapper.style.setProperty('height', designHeight + 'px', 'important');
        contentWrapper.style.setProperty('max-width', designWidth + 'px', 'important');
        contentWrapper.style.setProperty('max-height', designHeight + 'px', 'important');
        contentWrapper.style.setProperty('min-width', designWidth + 'px', 'important');
        contentWrapper.style.setProperty('min-height', designHeight + 'px', 'important');

        contentWrapper.style.removeProperty('zoom');

        if (scale < 1.0) {
            contentWrapper.style.setProperty('transform', `scale(${scale})`, 'important');
            contentWrapper.style.setProperty('transform-origin', 'center center', 'important');
        } else {
            contentWrapper.style.removeProperty('transform');
            contentWrapper.style.removeProperty('transform-origin');
        }

        updateDebugInfo(windowWidth, windowHeight, scale, contentWrapper, devicePixelRatio, 1.0);
    }
}

// 更新调试信息显示（按D键切换）
function updateDebugInfo(w, h, scale, wrapper, devicePixelRatio, antiDpiZoom) {
    const debugInfo = document.getElementById('debugInfo');
    if (!debugInfo) return;

    const computed = window.getComputedStyle(wrapper);
    const transform = computed.transform === 'none' ? 'none' : computed.transform;
    const zoom = computed.zoom || '1';

    document.getElementById('debugInnerWidth').textContent = window.innerWidth + 'px';
    document.getElementById('debugInnerHeight').textContent = window.innerHeight + 'px';
    document.getElementById('debugOuterWidth').textContent = window.outerWidth + 'px';
    document.getElementById('debugOuterHeight').textContent = window.outerHeight + 'px';
    document.getElementById('debugScreenWidth').textContent = window.screen.width + 'px';
    document.getElementById('debugScreenHeight').textContent = window.screen.height + 'px';
    document.getElementById('debugAvailWidth').textContent = window.screen.availWidth + 'px';
    document.getElementById('debugAvailHeight').textContent = window.screen.availHeight + 'px';

    const dpiInfo = document.getElementById('debugDpiInfo');
    if (dpiInfo) {
        dpiInfo.textContent = `devicePixelRatio: ${devicePixelRatio.toFixed(2)} (系统缩放${(devicePixelRatio * 100).toFixed(0)}%), 抵消zoom: ${antiDpiZoom.toFixed(3)}`;
    }

    document.getElementById('debugWindowSize').textContent = `${w} × ${h}`;
    document.getElementById('debugScale').textContent = scale.toFixed(3);
    document.getElementById('debugTransform').textContent = transform;

    const zoomInfo = document.getElementById('debugZoom');
    if (zoomInfo) {
        zoomInfo.textContent = zoom;
    }

    debugInfo.style.display = window.showDebugInfo ? 'block' : 'none';
}
//#endregion

//#region ==================== 页面初始化模块 ====================

// 页面初始化入口（登录后调用）
async function init() {
    try {
        const mainContainer = document.querySelector('.main-container');
        if (mainContainer) {
            mainContainer.style.display = 'flex';
        }

        if (typeof checkAuth === 'function') {
            const isAuth = await checkAuth();
            if (!isAuth) {
                return;
            }
        } else {
            if (!StorageManager.session.getUserToken()) {
                return;
            }
        }
    } catch (error) {
    }

    if (typeof updateCounts === 'function') {
        updateCounts();
    }
    if (typeof initGlobalStatsDisplay === 'function') {
        initGlobalStatsDisplay();
    }
    if (typeof updateButtonState === 'function') {
        updateButtonState();
    }
    
    if (typeof updateConnectionStatus === 'function') {
        updateConnectionStatus(false);
    }
    if (typeof resetInboxOnConnect === 'function') {
        resetInboxOnConnect();
    }

    if (typeof connectToBackendWS === 'function') {
        connectToBackendWS();
    }

    if (typeof loadServersFromAPI === 'function') {
        loadServersFromAPI();
    }

    const statusList = document.getElementById('statusList');
    if (statusList) {
        statusList.innerHTML = '';
    }

    if (typeof updateIntervalColor === 'function') {
        updateIntervalColor();
    }

    const intervalSelect = document.getElementById('intervalInput');
    if (intervalSelect && typeof updateIntervalColor === 'function') {
        intervalSelect.addEventListener('change', updateIntervalColor);
    }

    if (typeof updateNotificationCount === 'function') {
        updateNotificationCount();
    }

    if (typeof setupExpand === 'function') {
        setupExpand('numTrigger', 'numWrapper');
        setupExpand('logTrigger', 'logWrapper');
    }

    const panelB = document.getElementById('panelB');
    if (panelB) {
        panelB.classList.remove('mobile-show');
        panelB.style.display = 'none';
    }

    if (typeof updateScale === 'function') {
        updateScale();
        window.addEventListener('resize', updateScale);
    }

    window.showDebugInfo = false;
    document.addEventListener('keydown', function (e) {
        if (e.key === 'd' || e.key === 'D') {
            window.showDebugInfo = !window.showDebugInfo;
            const debugInfo = document.getElementById('debugInfo');
            if (debugInfo) {
                debugInfo.style.display = window.showDebugInfo ? 'block' : 'none';
            }
        }
    });

    if (typeof switchPanel === 'function') {
        switchPanel('home');
    }
}

//#endregion
