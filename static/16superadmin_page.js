//#region ==================== 超级管理员面板 ====================

// 当前选中的Worker（必须在函数使用前声明）
let currentRemoteWorkerId = null;
let currentRemoteWorkerData = null;

// 费率设置相关变量（提前声明）
let saCurrentAdminId = null;
let saCurrentUserId = null;
let saGlobalEditing = false;

function saSafeSetText(id, value) {
    const el = document.getElementById(id);
    if (el) el.textContent = value;
}

//#endregion
//#region ==================== 提示气泡组件 ====================
// 提示气泡定时器
let saToastTimer = null;

// 显示提示气泡（成功/错误/警告）
function saShowToast(message, type = 'info') {
    const toast = document.getElementById('saToast');
    if (!toast) return;

    // 清除之前的定时器
    if (saToastTimer) {
        clearTimeout(saToastTimer);
    }

    // 设置消息
    toast.textContent = message;

    // 清除所有类型类
    toast.classList.remove('success', 'error', 'warning');

    // 添加类型类
    if (type === 'success') {
        toast.classList.add('success');
    } else if (type === 'error') {
        toast.classList.add('error');
    } else if (type === 'warning') {
        toast.classList.add('warning');
    }

    // 显示气泡
    toast.classList.add('show');

    // 2秒后自动消失
    saToastTimer = setTimeout(() => {
        toast.classList.remove('show');
    }, 2000);
}

if (typeof SERVER_BOT_HTML === 'undefined') {
    const SERVER_BOT_HTML = `
        <div class="bot-container">
            <div class="signals">
                <div class="signal-ring"></div>
                <div class="signal-ring"></div>
                <div class="signal-ring"></div>
            </div>
            <div class="radar-bot">
                <div class="dish-assembly">
                    <div class="dish-head">
                        <div class="dish-inner"></div>
                        <div class="dish-antenna"></div>
                    </div>
                </div>
                <div class="body-unit">
                    <div class="face-screen">
                        <div class="eye"></div>
                        <div class="eye"></div>
                    </div>
                    <div class="tech-line"></div>
                </div>
                <div class="base-unit"></div>
                <div class="thruster-glow"></div>
            </div>
        </div>
    `;
}

//#endregion
//#region ==================== 密码验证与面板逻辑 ====================
// 当前选中的服务器ID
let currentSuperAdminServerId = null;
// 服务器列表数据
let superAdminServers = [];
let superAdminServersRefreshTimer = null;
function isOnlineStatus(status) {
    const s = String(status || '').toLowerCase();
    return s === 'ok' || s === 'connected' || s === 'online' || s === 'available' || s === 'ready';
}

function isSuperAdminServerOnline(server, now = Date.now()) {
    if (!server || !isOnlineStatus(server.status)) {
        return false;
    }
    if (!server.last_seen) {
        return false;
    }
    const lastSeen = new Date(server.last_seen).getTime();
    if (!Number.isFinite(lastSeen) || lastSeen <= 0) {
        return false;
    }
    return (now - lastSeen) <= 120000;
}
// 检查变量是否已声明，避免重复声明错误
// 使用 window 对象来避免重复声明错误
if (typeof window.currentManagerId === 'undefined') {
    window.currentManagerId = null;
}
if (typeof window.managerUsers === 'undefined') {
    window.managerUsers = [];
}
if (typeof window.managerUserGroups === 'undefined') {
    window.managerUserGroups = [];
}
if (typeof window.adminAccounts === 'undefined') {
    window.adminAccounts = [];
}

// 显示超级管理员密码输入弹窗
function showSuperAdminPasswordModal() {
    const modal = document.getElementById('superAdminPasswordModal');
    if (!modal) return;

    // 每次进入都需要输入密码
    const passwordInput = document.getElementById('superAdminPasswordInput');
    if (passwordInput) {
        passwordInput.value = '';
    }

    modal.style.display = 'flex';
    requestAnimationFrame(() => {
        modal.classList.add('show');
        setTimeout(() => {
            if (passwordInput) {
                passwordInput.focus();
            }
        }, 50);
    });
}

// 关闭超级管理员密码输入弹窗
function closeSuperAdminPasswordModal() {
    const modal = document.getElementById('superAdminPasswordModal');
    if (!modal) return;

    modal.classList.remove('show');
    setTimeout(() => {
        modal.style.display = 'none';
        const passwordInput = document.getElementById('superAdminPasswordInput');
        if (passwordInput) {
            passwordInput.value = '';
        }
    }, 200);
}

async function verifySuperAdminPassword() {
    const password = document.getElementById('superAdminPasswordInput').value.trim();

    if (!password) {
        saShowToast('请输入密码', 'warning');
        return;
    }

    // 通过API验证密码
    try {
        const response = await fetch(`${API_BASE_URL}/server-manager/login`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ password: password })
        });

        const data = await response.json();

        if (response.ok && data.success && data.token) {
            // 🔑 保存token到sessionStorage用于本次会话的API调用
            // 使用 server_manager_token 与后端验证逻辑保持一致
            if (data.token) {
                sessionStorage.setItem('server_manager_token', data.token);
            }
            closeSuperAdminPasswordModal();
            showSuperAdminPanel();
        } else {
            saShowToast(data.message || '密码错误', 'error');
        }
    } catch (e) {
        saShowToast('登录失败，请检查网络连接', 'error');
    }
}

function showSuperAdminPanel() {
    const panel = document.getElementById('superAdminPanel');
    if (!panel) return;

    panel.style.display = 'flex';
    requestAnimationFrame(() => {
        panel.classList.add('show');
        loadSuperAdminServers();
        setupSuperAdminLogControls();
        if (superAdminServersRefreshTimer) {
            clearInterval(superAdminServersRefreshTimer);
        }
        superAdminServersRefreshTimer = setInterval(() => {
            loadSuperAdminServers();
        }, 30000);
    });
}




function closeSuperAdminPanel() {
    const panel = document.getElementById('superAdminPanel');
    if (!panel) return;

    panel.classList.remove('show');
    setTimeout(() => {
        panel.style.display = 'none';
        currentSuperAdminServerId = null;
        const detailSection = document.getElementById('superAdminDetailSection');
        if (detailSection) {
            detailSection.style.display = 'none';
        }
        if (superAdminServersRefreshTimer) {
            clearInterval(superAdminServersRefreshTimer);
            superAdminServersRefreshTimer = null;
        }
    }, 200);
}
//#endregion
//#region ==================== 服务器列表管理 ======================
async function loadSuperAdminServers() {
    try {
        const response = await fetch(`${API_BASE_URL}/servers?t=${Date.now()}`, {
            method: 'GET',
            headers: { 'Content-Type': 'application/json' }
        });

        if (!response.ok) {
            throw new Error(`API响应错误: ${response.status}`);
        }

        const data = await response.json();
        if (data.success && data.servers) {
            superAdminServers = data.servers.map(s => ({ ...s }));
            renderSuperAdminServers();
        }
    } catch (error) {
        appendSuperAdminLog(`加载服务器列表失败: ${error.message}`, 'error');
    }
}

// 切换子菜单展开/折叠
function toggleSubmenu(submenuId, btn) {
    const submenu = document.getElementById(submenuId);
    if (!submenu) return;

    // 切换展开状态
    submenu.classList.toggle('expanded');
    btn.classList.toggle('expanded');
}

// 重置所有面板到初始状态
function saResetAllPanels() {
    // 关闭Worker控制面板
    closeWorkerRemotePanel();

    // 重置费率设置面板
    // 全局费率
    saGlobalEditing = false;
    const globalInputs = document.querySelectorAll('#saGlobalDisplay .num-input');
    globalInputs.forEach(input => {
        input.readOnly = true;
        input.classList.remove('editing');
    });
    const globalEditBtn = document.getElementById('saGlobalEditBtn');
    const globalSaveBtn = document.getElementById('saGlobalSaveBtn');
    const globalCancelBtn = document.getElementById('saGlobalCancelBtn');
    const globalResetBtn = document.getElementById('saGlobalResetBtn');
    if (globalEditBtn) globalEditBtn.style.display = 'block';
    if (globalSaveBtn) globalSaveBtn.style.display = 'none';
    if (globalCancelBtn) globalCancelBtn.style.display = 'none';
    if (globalResetBtn) globalResetBtn.style.display = 'none';

    // 管理员费率范围
    saCurrentAdminId = null;
    const salesInputArea = document.getElementById('saSalesInputArea');
    const salesSettingArea = document.getElementById('saSalesSettingArea');
    const salesSearchInput = document.getElementById('saSalesSearchUser');
    if (salesInputArea) salesInputArea.style.display = 'none';
    if (salesSettingArea) {
        salesSettingArea.style.display = 'none';
        salesSettingArea.classList.remove('show');
    }
    if (salesSearchInput) salesSearchInput.value = '';
    saResetSales();

    // 用户费率
    saCurrentUserId = null;
    const userInputArea = document.getElementById('saUserInputArea');
    const userSettingArea = document.getElementById('saUserSettingArea');
    const userSearchInput = document.getElementById('saUserSearchName');
    if (userInputArea) userInputArea.style.display = 'none';
    if (userSettingArea) {
        userSettingArea.style.display = 'none';
        userSettingArea.classList.remove('show');
    }
    if (userSearchInput) userSearchInput.value = '';
    saResetUser();

    // 重置充值面板
    if (typeof saResetRecharge === 'function') {
        saResetRecharge();
    }

    // 重置数据面板搜索和筛选
    const dataUserSearch = document.getElementById('saDataUserSearch');
    const dataAdminSearch = document.getElementById('saDataAdminSearch');
    if (dataUserSearch) dataUserSearch.value = '';
    if (dataAdminSearch) dataAdminSearch.value = '';

    // 停止日志自动刷新
    stopLogAutoRefresh();
}

// 切换超级管理员面板Tab页
function switchSuperAdminTab(tab, subOption = null) {
    // 0. 重置所有面板到初始状态（切换前清理）
    saResetAllPanels();

    // 1. Update Sidebar Buttons - 清除所有高亮状态（主菜单和子菜单）
    document.querySelectorAll('.super-admin-sidebar .sidebar-btn').forEach((btn) => btn.classList.remove('active'));
    document.querySelectorAll('.sidebar-submenu-btn').forEach(btn => {
        btn.classList.remove('active');
    });

    // 如果是子菜单点击，只高亮子菜单按钮
    if (subOption) {
        const subBtn = document.querySelector(
            `.sidebar-submenu-btn[onclick*="'${subOption}'"]`
        );
        if (subBtn) subBtn.classList.add('active');

        // 展开对应的子菜单（如果未展开）
        if (tab === 'users' || tab === 'logs') {
            const expandableBtns = document.querySelectorAll('.sidebar-btn.expandable');
            expandableBtns.forEach(btn => {
                const submenuId = btn.getAttribute('onclick').match(/toggleSubmenu\('(\w+)'/);
                if (submenuId) {
                    const submenu = document.getElementById(submenuId[1]);
                    if (submenu && submenu.querySelector(`[onclick*="'${subOption}'"]`)) {
                        submenu.classList.add('expanded');
                        btn.classList.add('expanded');
                    }
                }
            });
        }
    } else {
        // 如果是主菜单点击，只高亮主菜单按钮
        const activeBtn = document.querySelector(
            `.super-admin-sidebar .sidebar-btn[onclick*="'${tab}'"]`
        );
        if (activeBtn) activeBtn.classList.add('active');
    }

    // 2. Hide All Main Sections
    const sections = [
        'superAdminServersSection',
        'superAdminUserSection',
        'superAdminRechargeSection',
        'superAdminDetailSection',
        'superAdminRatesSection',
        'superAdminLogsSection'
    ];
    sections.forEach((id) => {
        const el = document.getElementById(id);
        if (el) el.style.display = 'none';
    });

    // 3. Show Target Section & logic
    if (tab === 'servers' || tab === 'default') {
        const el = document.getElementById('superAdminServersSection');
        if (el) el.style.display = 'block';
        if (typeof loadSuperAdminServers === 'function') loadSuperAdminServers();
        // Also show radar
        const radar = document.querySelector('.servers-radar-section');
        if (radar) radar.style.display = 'block';
    } else if (tab === 'users') {
        const el = document.getElementById('superAdminUserSection');
        if (el) el.style.display = 'block';
        // 隐藏原来的顶栏 tab 按钮，使用子菜单切换
        const tabs = el.querySelector('.sa-data-tabs');
        if (tabs) tabs.style.display = 'none';
        // 根据子选项切换
        if (subOption) {
            saSwitchDataTab(subOption);
            // 刷新标题
            const subNames = { 'user': '用户数据', 'admin': '管理员数据', 'server': '服务器数据' };
            document.querySelector('.header-title span:last-child').textContent = subNames[subOption] || '数据管理';
        }
    } else if (tab === 'recharge') {
        const el = document.getElementById('superAdminRechargeSection');
        if (el) el.style.display = 'block';
        // 初始化充值面板状态
        if (typeof saResetRecharge === 'function') {
            saResetRecharge();
        }
    } else if (tab === 'rates') {
        const el = document.getElementById('superAdminRatesSection');
        if (el) el.style.display = 'block';
        // 加载全局费率
        saLoadGlobalRates();
    } else if (tab === 'logs') {
        const el = document.getElementById('superAdminLogsSection');
        if (el) el.style.display = 'block';
        const logContent = document.getElementById('superAdminLogContent');
        if (logContent) logContent.innerHTML = '';

        // 根据子选项加载对应的日志类型（INFO、SYSTEM 或 ACCESS）
        const logType = subOption || 'INFO';
        window.currentLogType = logType;

        // 更新当前日志类型标签
        const typeLabel = document.getElementById('currentLogTypeLabel');
        if (typeLabel) {
            typeLabel.textContent = logType === 'ACCESS' ? '访问记录' : logType;
        }

        // 停止之前的自动刷新
        stopLogAutoRefresh();

        // 根据类型加载不同内容
        if (logType === 'ACCESS') {
            // 加载访问记录
            loadAccessLogs();
        } else {
            // 初始化日志面板并加载INFO/SYSTEM日志
            initLogPanel();
            loadLogsByType(logType);

            // 启动实时自动刷新（仅当面板打开时）
            startLogAutoRefresh();
        }
    } else {
        // 切换到其他tab时停止自动刷新
        stopLogAutoRefresh();
    }
}

// 日志自动刷新控制
let logAutoRefreshInterval = null;
let logAutoRefreshEnabled = false;
let allLogsCache = []; // 缓存所有日志用于搜索

// 搜索和筛选日志
function filterLogs() {
    const searchInput = document.getElementById('logSearchInput');
    const sourceFilter = document.getElementById('logSourceFilter');
    const logContent = document.getElementById('superAdminLogContent');

    if (!logContent || !window.currentLogData) return;

    const searchTerm = searchInput ? searchInput.value.toLowerCase() : '';
    const sourceValue = sourceFilter ? sourceFilter.value : 'all';

    // 筛选日志（级别筛选由左侧 INFO/SYSTEM 分类处理）
    let filteredLogs = window.currentLogData.filter(log => {
        // 搜索内容筛选
        const message = (log.message || log.msg || '').toLowerCase();
        const matchesSearch = !searchTerm || message.includes(searchTerm);

        // 来源筛选
        const source = (log.source || '').toLowerCase();
        const matchesSource = sourceValue === 'all' || source === sourceValue;

        return matchesSearch && matchesSource;
    });

    // 更新统计面板
    updateLogStats(filteredLogs);

    // 重新渲染日志列表
    renderLogList(filteredLogs);
}

// 更新统计面板
function updateLogStats(logs) {
    const totalEl = document.getElementById('logStatTotal');
    const errorRateEl = document.getElementById('logStatErrorRate');
    const topSourceEl = document.getElementById('logStatTopSource');

    if (!totalEl || !logs) return;

    const total = logs.length;
    const errors = logs.filter(log => log.level === 'ERROR').length;
    const errorRate = total > 0 ? ((errors / total) * 100).toFixed(1) : 0;

    // 统计来源
    const sourceCount = {};
    logs.forEach(log => {
        const source = log.source || 'unknown';
        sourceCount[source] = (sourceCount[source] || 0) + 1;
    });

    let topSource = '-';
    let maxCount = 0;
    for (const [source, count] of Object.entries(sourceCount)) {
        if (count > maxCount) {
            maxCount = count;
            topSource = source.toUpperCase();
        }
    }

    // 更新显示
    totalEl.textContent = total;
    errorRateEl.textContent = errorRate + '%';
    errorRateEl.style.color = errorRate > 10 ? '#ff5252' : (errorRate > 5 ? '#ffd93d' : '#00ff88');
    topSourceEl.textContent = topSource;
}

// 渲染日志列表
function renderLogList(logs) {
    const logContent = document.getElementById('superAdminLogContent');
    if (!logContent) return;

    logContent.innerHTML = '';

    if (logs.length === 0) {
        logContent.innerHTML = '<div style="color: #888; text-align: center; padding: 20px;">没有匹配的日志</div>';
        return;
    }

    logs.forEach(log => {
        const logEntry = document.createElement('div');
        const displayLevel = getLogLevelDisplay(log.level);
        const levelClass = getLogLevelClass(log.level);
        logEntry.className = `log-line ${levelClass}`;

        const timestampStr = new Date(log.ts || Date.now()).toLocaleString('zh-CN', {
            month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit'
        });

        const sourceTag = getSourceTag(log.source);
        const message = log.message || log.msg || 'No message';

        logEntry.innerHTML = `<span style="color: #888;">[${timestampStr}]</span> ${sourceTag} <span class="${levelClass}">${displayLevel}</span> ${message}`;
        logContent.appendChild(logEntry);
    });
}

// 启动日志自动刷新
function startLogAutoRefresh() {
    // 如果已经在运行，先停止
    stopLogAutoRefresh();

    // 检查用户是否开启了自动刷新（默认关闭，需要手动开启）
    const autoRefreshBtn = document.getElementById('btnLogAutoRefresh');
    if (autoRefreshBtn && autoRefreshBtn.classList.contains('active')) {
        logAutoRefreshEnabled = true;
        // 每10秒刷新一次
        logAutoRefreshInterval = setInterval(() => {
            const type = window.currentLogType || 'INFO';
            loadLogsByType(type);
        }, 10000);
    }
}

// 停止日志自动刷新
function stopLogAutoRefresh() {
    if (logAutoRefreshInterval) {
        clearInterval(logAutoRefreshInterval);
        logAutoRefreshInterval = null;
        logAutoRefreshEnabled = false;
    }
}

// 切换日志自动刷新开关
function toggleLogAutoRefresh() {
    const btn = document.getElementById('btnLogAutoRefresh');
    if (!btn) return;

    btn.classList.toggle('active');
    const isActive = btn.classList.contains('active');

    if (isActive) {
        btn.textContent = '自动刷新: ON';
        btn.style.background = 'linear-gradient(135deg, #00ff88 0%, #00e676 100%)';
        // 如果当前在日志面板，立即启动
        const logSection = document.getElementById('superAdminLogsSection');
        if (logSection && logSection.style.display !== 'none') {
            startLogAutoRefresh();
        }
        saShowToast('自动刷新已开启', 'success');
    } else {
        btn.textContent = '自动刷新: OFF';
        btn.style.background = 'linear-gradient(135deg, #666 0%, #444 100%)';
        stopLogAutoRefresh();
        saShowToast('自动刷新已关闭', 'info');
    }
}

// 渲染超级管理员服务器列表
function renderSuperAdminServers() {
    const container = document.getElementById('superAdminServersList');
    if (!container) return;

    container.innerHTML = '';

    if (superAdminServers.length === 0) {
        container.innerHTML = '<div style="padding: 20px; text-align: center; color: #999; grid-column: 1 / -1;">暂无服务器</div>';
        return;
    }

    superAdminServers.forEach(server => {
        const btn = document.createElement('button');
        const serverId = server.server_id || server.server_name || 'Unknown';
        const portMatch = (server.url || '').match(/:(\d+)/);
        const port = portMatch ? portMatch[1] : (server.port || serverId.match(/\d+/)?.[0] || '?');

        const status = determineWorkerStatus(server);
        const isOnline = isSuperAdminServerOnline(server, Date.now());

        btn.className = `server-button ${isOnline ? 'connected' : 'disconnected'} super-admin-btn`;

        if (currentSuperAdminServerId === serverId) {
            btn.classList.add('selected');
        }

        btn.classList.add(`status-${status.type}`);

        const botHTML = SERVER_BOT_HTML;

        btn.innerHTML = `
                        ${botHTML}
                        <div class="server-button-name" style="position: absolute; bottom: -18px; left: 50%; transform: translateX(-50%); font-size: 14px; color: ${isOnline ? '#4facfe' : '#888'}; white-space: nowrap; pointer-events: none; z-index: 100; font-weight: 500; text-shadow: 0 1px 3px rgba(0,0,0,0.5);">${serverId}</div>
                        <div class="radar-bot-status ${status.type}" title="${status.text}" style="position: absolute; bottom: 5px; right: 5px; width: 14px; height: 14px; border-radius: 50%; border: 2px solid #1a1a2e;"></div>
                        <div class="server-tooltip">
                            <div style="font-weight: bold; margin-bottom: 4px;">${serverId}</div>
                            <div style="font-size: 11px; opacity: 0.9;">${server.url || ''}</div>
                            <div style="font-size: 11px; color: #00ff88; margin-top: 4px;">ID: ${serverId}</div>
                            <div style="font-size: 11px; color: ${status.type === 'good' ? '#00ff88' : status.type === 'error' ? '#ff5252' : '#ffd93d'}; margin-top: 2px;">Status: ${status.text}</div>
                            <div style="font-size: 11px; color: ${isOnline ? '#00ff88' : '#ff5252'}; margin-top: 2px;">${isOnline ? 'Online' : 'Offline'}</div>
                            ${server.bound_manager ? `<div style="font-size: 11px; color: #ff9800; margin-top: 2px;">Assigned to: ${server.bound_manager}</div>` : ''}
                        </div>
                        <span class="control-panel-btn" data-server-id="${serverId}">控制面板</span>
                    `;

        btn.addEventListener('click', (e) => {
            if (e.target.classList.contains('control-panel-btn')) {
                return;
            }
            document.querySelectorAll('.super-admin-btn').forEach(b => b.classList.remove('selected'));
            btn.classList.add('selected');
        });

        const controlBtn = btn.querySelector('.control-panel-btn');
        if (controlBtn) {
            controlBtn.addEventListener('click', (e) => {
                e.stopPropagation();
                e.preventDefault();
                document.querySelectorAll('.super-admin-btn').forEach(b => b.classList.remove('selected'));
                btn.classList.add('selected');
                if (typeof openWorkerRemotePanel === 'function') {
                    openWorkerRemotePanel(serverId);
                }
            });
        }
        container.appendChild(btn);
    });

    if (typeof initRadarBots === 'function') {
        setTimeout(initRadarBots, 50);
    }
}

// 选择超级管理员服务器
async function selectSuperAdminServer(serverId) {
    currentSuperAdminServerId = serverId;

    // 更新按钮状态
    document.querySelectorAll('.super-admin-server-btn').forEach(btn => {
        btn.classList.remove('active');
        if (btn.textContent === serverId) {
            btn.classList.add('active');
        }
    });

    // 获取服务器详细信息
    try {
        const response = await fetch(`${API_BASE_URL}/super-admin/worker/${encodeURIComponent(serverId)}/info`, {
            method: 'GET',
            headers: { 'Content-Type': 'application/json' }
        });

        if (response.ok) {
            const data = await response.json();
            if (data.success) {
                displaySuperAdminServerInfo(data.info);
                const detailSection = document.getElementById('superAdminDetailSection');
                if (detailSection) {
                    detailSection.style.display = 'flex';
                }
            } else {
                appendSuperAdminLog(`获取服务器信息失败: ${data.message}`, 'error');
            }
        } else {
            // 如果API接口不存在，从本地数据获取
            const server = superAdminServers.find(s => (s.server_id || s.server_name) === serverId);
            if (server) {
                displaySuperAdminServerInfoFromData(server);
                const detailSection = document.getElementById('superAdminDetailSection');
                if (detailSection) {
                    detailSection.style.display = 'flex';
                }
            }
        }
    } catch (error) {
        // 如果API接口不存在，从本地数据获取
        const server = superAdminServers.find(s => (s.server_id || s.server_name) === serverId);
        if (server) {
            displaySuperAdminServerInfoFromData(server);
            const detailSection = document.getElementById('superAdminDetailSection');
            if (detailSection) {
                detailSection.style.display = 'flex';
            }
        } else {
            appendSuperAdminLog(`获取服务器信息失败: ${error.message}`, 'error');
        }
    }
}
//#endregion
//#region ==================== 服务器详情展示 ====================
// 显示服务器信息（从API数据）
function displaySuperAdminServerInfo(info) {
    const serverIdEl = document.getElementById('opPanelServerId');
    const numberEl = document.getElementById('superAdminNumber');
    const emailEl = document.getElementById('superAdminEmail');
    const portEl = document.getElementById('superAdminPort');
    const apiEl = document.getElementById('superAdminApi');
    const statusBtn = document.getElementById('superAdminServerStatusBtn');

    if (serverIdEl) serverIdEl.textContent = 'ID: ' + (info.server_id || info.server_name || '-');
    if (numberEl) numberEl.textContent = (info.meta && info.meta.phone) || '-';
    if (emailEl) emailEl.textContent = (info.meta && info.meta.email) || '-';
    if (portEl) portEl.textContent = info.port || '-';
    if (apiEl) apiEl.textContent = info.api_url || '-';

    if (statusBtn) {
        if (info.status === 'connected' || info.status === 'available') {
            statusBtn.textContent = 'Stop Server';
            statusBtn.classList.remove('primary');
            statusBtn.classList.add('danger', 'running');
        } else {
            statusBtn.textContent = 'Start Server';
            statusBtn.classList.remove('danger', 'running');
            statusBtn.classList.add('primary');
        }
    }
}

// 显示服务器信息（从本地数据）
function displaySuperAdminServerInfoFromData(server) {
    const meta = server.meta || {};
    const serverIdEl = document.getElementById('opPanelServerId');
    const numberEl = document.getElementById('superAdminNumber');
    const emailEl = document.getElementById('superAdminEmail');
    const portEl = document.getElementById('superAdminPort');
    const apiEl = document.getElementById('superAdminApi');
    const statusBtn = document.getElementById('superAdminServerStatusBtn');

    if (serverIdEl) serverIdEl.textContent = 'ID: ' + (server.server_id || server.server_name || '-');
    if (numberEl) numberEl.textContent = meta.phone || '-';
    if (emailEl) emailEl.textContent = meta.email || '-';
    if (portEl) portEl.textContent = server.port || '-';
    if (apiEl) apiEl.textContent = server.server_url || '-';

    if (statusBtn) {
        const status = (server.status || '').toLowerCase();
        if (status === 'connected' || status === 'available') {
            statusBtn.textContent = 'Stop Server';
            statusBtn.classList.remove('primary');
            statusBtn.classList.add('danger', 'running');
        } else {
            statusBtn.textContent = 'Start Server';
            statusBtn.classList.remove('danger', 'running');
            statusBtn.classList.add('primary');
        }
    }
}
//#endregion
//#region ==================== 远程指令控制 ====================
// 切换服务器运行状态
async function toggleSuperAdminServer() {
    if (!currentSuperAdminServerId) {
        appendSuperAdminLog('请先选择服务器', 'warning');
        return;
    }

    const statusBtn = document.getElementById('superAdminServerStatusBtn');
    const isRunning = statusBtn && statusBtn.classList.contains('running');
    const action = isRunning ? 'stop_server' : 'start_server';

    await sendSuperAdminCommand(action);
}

// 发送超级管理员命令
async function sendSuperAdminCommand(action, params = {}) {
    // 使用 currentRemoteWorkerId（点击面板时设置）或 currentSuperAdminServerId
    const serverId = currentRemoteWorkerId || currentSuperAdminServerId;
    if (!serverId) {
        appendWorkerConsole('请先选择服务器', 'error');
        return;
    }

    appendWorkerConsole(`执行命令: ${action}...`, 'command');
    showLoading();

    try {
        const response = await fetch(`${API_BASE_URL}/super-admin/worker/${encodeURIComponent(serverId)}/control`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ action, params })
        });

        const data = await response.json();
        if (data.success) {
            appendWorkerConsole(`命令已发送，等待执行结果...`, 'info');
            hideLoading();
        } else {
            appendWorkerConsole(`命令发送失败: ${data.message || '未知错误'}`, 'error');
            hideLoading();
        }
    } catch (error) {
        appendWorkerConsole(`发送命令失败: ${error.message}`, 'error');
        hideLoading();
    }
}

// Worker 控制面板命令函数（别名）
// 发送Worker命令
async function sendWorkerCommand(action, params = {}) {
    await sendSuperAdminCommand(action, params);
}
//#endregion
//#region ==================== 日志系统 ====================
// 加载历史日志
async function loadHistoryLogs() {
    const logContent = document.getElementById('superAdminLogContent');
    if (!logContent) {
        appendSuperAdminLog('日志容器未找到', 'error');
        return;
    }

    // 清空现有日志
    logContent.innerHTML = '';
    appendSuperAdminLog('正在加载历史日志...', 'info');

    try {
        const token = sessionStorage.getItem('server_manager_token') || '';
        if (!token) {
            appendSuperAdminLog('未登录或会话已过期，请重新输入密码', 'error');
            return;
        }
        const response = await fetch(`${API_BASE_URL}/admin/logs?limit=100`, {
            headers: { 'Authorization': 'Bearer ' + token }
        });
        const data = await response.json();
        if (data.ok) {
            const logs = normalizeAdminLogsPayload(data);
            // 清空并加载历史日志
            logContent.innerHTML = '';
            logs.reverse().forEach(log => {
                let type = 'info';
                if (log.level === 'WARN') type = 'warning';
                if (log.level === 'ERROR') type = 'error';
                const ts = log.ts ? new Date(log.ts).toLocaleTimeString('zh-CN') : '';

                const logEntry = document.createElement('div');
                logEntry.className = `log-line ${type}`;
                logEntry.textContent = `[${ts}] [${log.module || 'SYSTEM'}] ${log.message || ''}`;
                logContent.appendChild(logEntry);
            });
            logContent.scrollTop = logContent.scrollHeight;
            appendSuperAdminLog(`历史日志加载完毕 (共 ${logs.length} 条)`, 'success');
        } else {
            appendSuperAdminLog('获取历史日志失败: ' + (data.message || data.error || 'unknown'), 'error');
        }
    } catch (e) {
        appendSuperAdminLog('网络错误: ' + e.message, 'error');
    }
}

let currentLogLevel = null;

// 切换日志级别
// 切换日志级别
async function switchLogLevel(level) {
    currentLogLevel = level;
    const logContent = document.getElementById('superAdminLogContent');
    if (!logContent) return;

    // 更新按钮高亮状态
    document.querySelectorAll('.log-level-btn').forEach(btn => {
        btn.style.opacity = '0.6';
        btn.style.fontWeight = 'normal';
        btn.style.boxShadow = 'none';
    });

    const btnMap = {
        'all': 'btnLogLevel',
        'system': 'btnLogSystem',
        'error': 'btnLogError',
        'info': 'btnLogInfo',
        'record': 'btnLogRecord'
    };

    const activeBtn = document.getElementById(btnMap[level]);
    if (activeBtn) {
        activeBtn.style.opacity = '1';
        activeBtn.style.fontWeight = 'bold';
        activeBtn.style.boxShadow = '0 0 0 2px rgba(79, 172, 254, 0.5)';
    }

    // 前端日志直接加载
    if (level === 'frontend') {
        loadFrontendLogs();
    } else {
        logContent.innerHTML = '';
        await loadLogsByLevel(level);
    }
}

// 确保函数暴露到全局作用域
if (typeof window !== 'undefined') {
    window.switchLogLevel = switchLogLevel;
    window.toggleClearLogMenu = toggleClearLogMenu;
    window.confirmClearLogs = confirmClearLogs;
}

// 新增：按日志分类加载日志
// 按分类加载日志
async function loadLogsByCategory(category) {
    const c = String(category || '').toLowerCase();
    if (c === 'access' || c === 'record') {
        await loadAccessLogs();
        return;
    }
    if (c === 'all') {
        await loadLogsByType('ALL');
        return;
    }
    if (c === 'system' || c === 'error') {
        await loadLogsByType('SYSTEM');
        return;
    }
    await loadLogsByType('INFO');
}

// 按级别加载日志
function getServerManagerAuthToken() {
    if (typeof StorageManager !== 'undefined' && StorageManager.session) {
        const t = StorageManager.session.getServerManagerToken();
        if (t) return t;
    }
    return sessionStorage.getItem('server_manager_token') || '';
}

function normalizeAdminLogsPayload(data) {
    const toList = (arr, level) => (Array.isArray(arr) ? arr : []).map(item => ({
        ts: item.timestamp || item.ts || null,
        source: item.source || item.source_type || 'api',
        level: level,
        message: item.content || item.message || ''
    }));

    const info = toList(data && data.info, 'INFO');
    const error = toList(data && data.error, 'ERROR');
    return [...info, ...error].sort((a, b) => new Date(b.ts || 0) - new Date(a.ts || 0));
}

async function fetchUnifiedAdminLogs(limit = 1000, days = 3) {
    const token = getServerManagerAuthToken();
    if (!token) {
        throw new Error('未登录或会话已过期');
    }
    const res = await fetch(`${API_BASE_URL}/admin/logs?limit=${limit}&days=${days}`, {
        method: 'GET',
        headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${token}`
        }
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok || !data.ok) {
        throw new Error(data.message || `HTTP ${res.status}`);
    }
    return normalizeAdminLogsPayload(data);
}

async function loadLogsByLevel(type) {
    const mapped = String(type || '').toLowerCase();
    if (mapped === 'access') {
        await loadAccessLogs();
        return;
    }
    if (mapped === 'all') {
        await loadLogsByType('ALL');
        return;
    }
    if (mapped === 'system' || mapped === 'error') {
        await loadLogsByType('SYSTEM');
        return;
    }
    await loadLogsByType('INFO');
}

// 新增：根据日志类型加载日志（INFO 或 SYSTEM）
// 按类型加载日志
async function loadLogsByType(type) {
    const logContent = document.getElementById('superAdminLogContent');
    if (!logContent) return;

    try {
        const allLogs = await fetchUnifiedAdminLogs(1000, 3);
        const upperType = String(type || 'INFO').toUpperCase();
        let filteredLogs = allLogs;
        if (upperType === 'INFO') {
            filteredLogs = allLogs.filter(log => String(log.level || '').toUpperCase() === 'INFO');
        } else if (upperType === 'SYSTEM') {
            filteredLogs = allLogs.filter(log => String(log.level || '').toUpperCase() !== 'INFO');
        }

        // 按时间降序排列（最新在顶部）
        filteredLogs.sort((a, b) => {
            const dateA = new Date(a.ts || Date.now());
            const dateB = new Date(b.ts || Date.now());
            return dateB - dateA;
        });

        logContent.innerHTML = '';

        if (filteredLogs.length === 0) {
            logContent.innerHTML = '<div style="color: #888; text-align: center; padding: 20px;">暂无日志</div>';
            return;
        }

        // 存储当前日志数据用于清空、刷新、导出操作
        window.currentLogData = filteredLogs;
        allLogsCache = filteredLogs; // 保存到缓存用于搜索

        // 更新统计面板
        updateLogStats(filteredLogs);

        filteredLogs.forEach(log => {
            const logEntry = document.createElement('div');
            const displayLevel = getLogLevelDisplay(log.level);
            const levelClass = getLogLevelClass(log.level);
            logEntry.className = `log-line ${levelClass}`;

            // 处理时间戳格式
            const timestampStr = new Date(log.ts || Date.now()).toLocaleString('zh-CN', {
                month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit'
            });

            // 获取日志来源标记
            const sourceTag = getSourceTag(log.source);
            const message = log.message || log.msg || 'No message';

            logEntry.innerHTML = `<span style="color: #888;">[${timestampStr}]</span> ${sourceTag} <span class="${levelClass}">${displayLevel}</span> ${message}`;
            logContent.appendChild(logEntry);
        });

    } catch (e) {
        appendSuperAdminLog('网络错误: ' + e.message, 'error');
        const logContentEl = document.getElementById('superAdminLogContent');
        if (logContentEl) {
            logContentEl.innerHTML = `<div style="color:#ff5252;text-align:center;padding:20px;">加载日志失败: ${e.message}</div>`;
        }
    }
}

// 获取日志来源标记（带颜色）
// 获取日志来源标签
function getSourceTag(source) {
    const sourceLower = (source || '').toLowerCase();
    let color = '#888';
    let displaySource = 'unknown';

    if (sourceLower === 'worker') {
        color = '#ff5252'; // 红色
        displaySource = 'worker';
    } else if (sourceLower === 'html') {
        color = '#4facfe'; // 蓝色
        displaySource = 'html';
    } else if (sourceLower === 'api') {
        color = '#00ff88'; // 绿色
        displaySource = 'api';
    }

    return `<span style="color: ${color}; font-weight: bold;">[${displaySource}]</span>`;
}

// 加载访问记录
// 加载访问记录
async function loadAccessLogs() {
    const logContent = document.getElementById('superAdminLogContent');
    const statsPanel = document.getElementById('logStatsPanel');

    if (!logContent) return;

    // 隐藏统计面板（访问记录用自己的统计）
    if (statsPanel) statsPanel.style.display = 'none';

    logContent.innerHTML = '<div style="color: #888; text-align: center; padding: 20px;">加载访问记录...</div>';

    try {
        const token = getServerManagerAuthToken();
        if (!token) {
            logContent.innerHTML = '<div style="color: #ff5252; text-align: center; padding: 20px;">未登录或会话已过期</div>';
            return;
        }

        // 并行加载访问记录和统计
        const [logsResponse, statsResponse] = await Promise.all([
            fetch(`${API_BASE_URL}/admin/access-logs?limit=100`, {
                method: 'GET',
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization': 'Bearer ' + token
                }
            }),
            fetch(`${API_BASE_URL}/admin/access-logs/stats`, {
                method: 'GET',
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization': 'Bearer ' + token
                }
            })
        ]);

        const logsData = await logsResponse.json();
        const statsData = await statsResponse.json();

        if (!logsData.ok) {
            logContent.innerHTML = `<div style="color: #ff5252; text-align: center; padding: 20px;">加载失败: ${logsData.message || '未知错误'}</div>`;
            return;
        }

        // 显示统计信息
        let statsHtml = '';
        if (statsData.ok && statsData.today) {
            const today = statsData.today;
            statsHtml = `
                <div style="display: flex; gap: 15px; margin-bottom: 15px; padding: 15px; background: rgba(30, 41, 59, 0.6); border-radius: 10px; border: 1px solid rgba(79, 172, 254, 0.2);">
                    <div style="flex: 1; text-align: center; padding: 10px; background: rgba(0, 0, 0, 0.2); border-radius: 8px;">
                        <div style="font-size: 12px; color: #888; margin-bottom: 5px;">今日访问</div>
                        <div style="font-size: 24px; font-weight: bold; color: #4facfe;">${today.total_visits || 0}</div>
                        <div style="font-size: 11px; color: #666;">次</div>
                    </div>
                    <div style="flex: 1; text-align: center; padding: 10px; background: rgba(0, 0, 0, 0.2); border-radius: 8px;">
                        <div style="font-size: 12px; color: #888; margin-bottom: 5px;">独立IP</div>
                        <div style="font-size: 24px; font-weight: bold; color: #00ff88;">${today.unique_ips || 0}</div>
                        <div style="font-size: 11px; color: #666;">个</div>
                    </div>
                    <div style="flex: 1; text-align: center; padding: 10px; background: rgba(0, 0, 0, 0.2); border-radius: 8px;">
                        <div style="font-size: 12px; color: #888; margin-bottom: 5px;">活跃用户</div>
                        <div style="font-size: 24px; font-weight: bold; color: #ffd93d;">${today.unique_users || 0}</div>
                        <div style="font-size: 11px; color: #666;">人</div>
                    </div>
                </div>
            `;
        }

        // 显示访问记录列表
        const logs = logsData.logs || [];

        if (logs.length === 0) {
            logContent.innerHTML = statsHtml + '<div style="color: #888; text-align: center; padding: 20px;">暂无访问记录</div>';
            return;
        }

        let html = statsHtml + '<div style="margin-top: 10px;">';

        logs.forEach(log => {
            const timestamp = new Date(log.ts).toLocaleString('zh-CN', {
                month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit'
            });

            const userType = log.user_type === 'admin' ? '管理员' : '用户';
            const userTypeColor = log.user_type === 'admin' ? '#ff5252' : '#4facfe';
            const username = log.username || '匿名';
            const ip = log.ip_address || 'unknown';
            const endpoint = log.endpoint || '-';
            const method = log.method || 'GET';
            const userAgent = log.user_agent ? log.user_agent.substring(0, 50) + '...' : '未知设备';

            html += `
                <div style="padding: 12px; margin-bottom: 8px; background: rgba(30, 41, 59, 0.4); border-radius: 8px; border-left: 3px solid ${userTypeColor}; font-size: 12px; line-height: 1.6;">
                    <div style="display: flex; justify-content: space-between; margin-bottom: 5px;">
                        <span style="color: #888;">[${timestamp}]</span>
                        <span style="color: ${userTypeColor}; font-weight: bold;">[${userType}] ${username}</span>
                    </div>
                    <div style="display: flex; justify-content: space-between; margin-bottom: 5px;">
                        <span style="color: #ffd93d;">IP: ${ip}</span>
                        <span style="color: #00ff88;">${method} ${endpoint}</span>
                    </div>
                    <div style="color: #aaa; font-size: 11px;">${userAgent}</div>
                </div>
            `;
        });

        html += '</div>';
        logContent.innerHTML = html;

    } catch (e) {
        logContent.innerHTML = `<div style="color: #ff5252; text-align: center; padding: 20px;">加载失败: ${e.message}</div>`;
    }
}

// 初始化日志面板
// 初始化日志面板
function initLogPanel() {
    // 显示统计面板（INFO/SYSTEM模式）
    const statsPanel = document.getElementById('logStatsPanel');
    const searchFilterDiv = document.querySelector('#superAdminLogsSection > div:nth-child(2)');

    if (statsPanel) statsPanel.style.display = 'flex';
    if (searchFilterDiv) searchFilterDiv.style.display = 'flex';

    // 重置统计面板
    const totalEl = document.getElementById('logStatTotal');
    const errorRateEl = document.getElementById('logStatErrorRate');
    const topSourceEl = document.getElementById('logStatTopSource');

    if (totalEl) totalEl.textContent = '0';
    if (errorRateEl) {
        errorRateEl.textContent = '0%';
        errorRateEl.style.color = '#00ff88';
    }
    if (topSourceEl) topSourceEl.textContent = '-';

    // 清空搜索和筛选
    const searchInput = document.getElementById('logSearchInput');
    const sourceFilter = document.getElementById('logSourceFilter');

    if (searchInput) searchInput.value = '';
    if (sourceFilter) sourceFilter.value = 'all';
}

// 清空日志（只清空显示，不删除记录）
// 清空日志显示
function clearLogs() {
    const logContent = document.getElementById('superAdminLogContent');
    if (!logContent) return;

    logContent.innerHTML = '<div style="color: #888; text-align: center; padding: 20px;">日志显示已清空</div>';

    // 清空搜索框和筛选器
    const searchInput = document.getElementById('logSearchInput');
    const sourceFilter = document.getElementById('logSourceFilter');

    if (searchInput) searchInput.value = '';
    if (sourceFilter) sourceFilter.value = 'all';

    saShowToast('日志显示已清空', 'success');
}

// 刷新日志（获取最新数据显示）
// 刷新日志
async function refreshLogs() {
    const type = window.currentLogType || 'INFO';
    if (String(type).toUpperCase() === 'ACCESS') {
        await loadAccessLogs();
    } else {
        await loadLogsByType(type);
    }
    saShowToast('日志已刷新', 'success');
}

// 导出日志（导出为TXT格式，支持导出筛选后的结果）
// 导出日志为TXT文件
async function exportLogs() {
    const logContent = document.getElementById('superAdminLogContent');
    if (!logContent) return;

    // 获取当前显示的日志（已筛选的）
    const searchInput = document.getElementById('logSearchInput');
    const sourceFilter = document.getElementById('logSourceFilter');

    let logsToExport = window.currentLogData || [];

    // 如果有筛选条件，应用筛选
    if (searchInput && searchInput.value) {
        const searchTerm = searchInput.value.toLowerCase();
        logsToExport = logsToExport.filter(log => {
            const message = (log.message || log.msg || '').toLowerCase();
            return message.includes(searchTerm);
        });
    }

    if (sourceFilter && sourceFilter.value !== 'all') {
        const sourceValue = sourceFilter.value.toLowerCase();
        logsToExport = logsToExport.filter(log => (log.source || '').toLowerCase() === sourceValue);
    }

    if (logsToExport.length === 0) {
        saShowToast('没有可导出的日志', 'warning');
        return;
    }

    const type = window.currentLogType || 'INFO';

    try {
        // 生成 TXT 格式
        let txtContent = `=== 日志导出 ===\n`;
        txtContent += `类型: ${type}\n`;
        txtContent += `时间: ${new Date().toLocaleString('zh-CN')}\n`;
        txtContent += `数量: ${logsToExport.length} 条\n`;
        txtContent += `========================================\n\n`;

        logsToExport.forEach(log => {
            const timestampStr = new Date(log.ts || Date.now()).toLocaleString('zh-CN', {
                month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit'
            });
            const displayLevel = getLogLevelDisplay(log.level);
            const message = log.message || log.msg || 'No message';
            txtContent += `[${timestampStr}] [${(log.source || 'unknown').toUpperCase()}] [${displayLevel}] ${message}\n`;
        });

        // 创建下载链接
        const blob = new Blob([txtContent], { type: 'text/plain;charset=utf-8;' });
        const link = document.createElement('a');
        const url = URL.createObjectURL(blob);
        link.setAttribute('href', url);
        link.setAttribute('download', `logs_${type}_${new Date().toISOString().slice(0, 10).replace(/-/g, '')}.txt`);
        link.style.visibility = 'hidden';
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);

        saShowToast('日志已导出', 'success');
    } catch (e) {
        saShowToast('导出失败: ' + e.message, 'error');
    }
}

// 获取日志级别显示文本
function getLogLevelDisplay(level) {
    const levelUpper = (level || 'INFO').toUpperCase();
    switch (levelUpper) {
        case 'ERROR': return '[故障]';
        case 'FATAL': return '[致命]';
        case 'WARN': return '[警告]';
        case 'SYSTEM': return '[系统]';
        case 'INFO': return '[信息]';
        case 'DEBUG': return '[调试]';
        case 'TRACE': return '[跟踪]';
        default: return `[${levelUpper}]`;
    }
}

// 获取日志级别CSS类名
function getLogLevelClass(level) {
    const levelUpper = (level || 'INFO').toUpperCase();
    switch (levelUpper) {
        case 'ERROR': return 'level-error';
        case 'FATAL': return 'level-fatal';
        case 'WARN': return 'level-warn';
        case 'SYSTEM': return 'level-system';
        case 'INFO': return 'level-info';
        case 'DEBUG': return 'level-debug';
        case 'TRACE': return 'level-trace';
        default: return 'level-info';
    }
}

// 切换清空菜单
// 切换清空日志菜单
function toggleClearLogMenu() {
    const menu = document.getElementById('clearLogMenu');
    if (!menu) return;
    menu.style.display = menu.style.display === 'none' ? 'block' : 'none';
}

// 确认清空日志
// 确认清空日志
async function confirmClearLogs() {
    const selectedDayInput = document.querySelector('input[name="clearDays"]:checked');
    if (!selectedDayInput) {
        appendSuperAdminLog('请选择保留天数', 'error');
        return;
    }
    const days = parseInt(selectedDayInput.value);

    if (!confirm(`确定删除 ${days} 天前的所有日志？此操作不可恢复。`)) {
        return;
    }

    if (!confirm(`二次确认：即将删除 ${days} 天前的日志，确定继续？`)) {
        return;
    }

    try {
        const token = sessionStorage.getItem('server_manager_token') || '';
        const response = await fetch(`${API_BASE_URL}/admin/logs/clear?days=${days}`, {
            method: 'DELETE',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': 'Bearer ' + token
            }
        });

        const data = await response.json();
        if (data.ok) {
            appendSuperAdminLog(`✅ 已删除 ${days} 天前的日志 (共 ${data.deleted_count || 0} 条)`, 'success');
            toggleClearLogMenu();
            // 重新加载当前级别的日志
            if (currentLogLevel) {
                await loadLogsByLevel(currentLogLevel);
            }
        } else {
            appendSuperAdminLog(`删除失败: ${data.message || '未知错误'}`, 'error');
        }
    } catch (e) {
        appendSuperAdminLog(`删除失败: ${e.message}`, 'error');
    }
}



// 追加超级管理员日志
function appendSuperAdminLog(message, type = 'info') {
    // 优先使用日志面板的日志容器
    let logContent = document.getElementById('superAdminLogContent');
    // 如果日志面板不存在，使用详情面板的日志容器
    if (!logContent) {
        logContent = document.getElementById('superAdminDetailLogContent');
    }
    if (!logContent) return;

    const timestamp = new Date().toLocaleTimeString('zh-CN');
    const logEntry = document.createElement('div');
    logEntry.className = `log-line ${type}`;
    logEntry.textContent = `[${timestamp}] ${message}`;

    logContent.appendChild(logEntry);
    logContent.scrollTop = logContent.scrollHeight;
}

// 前端日志管理函数
// 加载前端日志
function loadFrontendLogs(filter = {}) {
    const logContent = document.getElementById('superAdminLogContent');
    if (!logContent) return;

    // 清空现有日志
    logContent.innerHTML = '';

    // 检查 Logger 是否可用
    if (typeof window.Logger === 'undefined' || typeof window.Logger.getMemoryLogs !== 'function') {
        appendSuperAdminLog('Logger 未初始化，无法加载前端日志', 'error');
        return;
    }

    // 获取前端日志
    const logs = window.Logger.getMemoryLogs(filter);

    if (logs.length === 0) {
        appendSuperAdminLog('暂无前端日志', 'info');
        return;
    }

    // 按时间降序排列（最新在顶部）
    const sortedLogs = logs.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));

    // 显示日志统计
    appendSuperAdminLog(`前端日志加载完成 (共 ${logs.length} 条)`, 'success');

    sortedLogs.forEach(log => {
        const logEntry = document.createElement('div');
        const timestamp = new Date(log.timestamp).toLocaleString('zh-CN', {
            month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit'
        });
        const levelClass = getLogLevelClass(log.level);
        const levelDisplay = getLogLevelDisplay(log.level);

        logEntry.className = `log-line ${levelClass}`;
        logEntry.innerHTML = `<span style="color: #888;">[${timestamp}]</span> <span class="${levelClass}">[${levelDisplay}]</span> <span style="color: #aaa;">[${log.category}]</span> ${log.message}`;

        logContent.appendChild(logEntry);
    });
}

// 清空前端日志
function clearFrontendLogs() {
    if (typeof window.Logger === 'undefined' || typeof window.Logger.clearMemoryLogs !== 'function') {
        appendSuperAdminLog('Logger 未初始化，无法清空前端日志', 'error');
        return;
    }

    const count = window.Logger.clearMemoryLogs();
    const logContent = document.getElementById('superAdminLogContent');
    if (logContent) {
        logContent.innerHTML = '';
    }
    appendSuperAdminLog(`已清空前端日志 (共 ${count} 条)`, 'success');

    // 重新加载以显示空状态
    loadFrontendLogs();
}

// 刷新前端日志
function refreshFrontendLogs() {
    loadFrontendLogs();
}

// 新增：切换日志分类
// 切换日志分类
function switchLogCategory(category) {
    // 更新分类按钮高亮状态
    document.querySelectorAll('.log-category-btn').forEach(btn => {
        btn.style.opacity = '0.6';
        btn.style.fontWeight = 'normal';
        btn.style.boxShadow = 'none';
    });

    const btnMap = {
        'all': 'btnLogCategoryAll',
        'info': 'btnLogCategoryInfo',
        'system': 'btnLogCategorySystem',
        'access': 'btnLogCategoryAccess',
        'transaction': 'btnLogCategoryTransaction'
    };

    const activeBtn = document.getElementById(btnMap[category]);
    if (activeBtn) {
        activeBtn.style.opacity = '1';
        activeBtn.style.fontWeight = 'bold';
        activeBtn.style.boxShadow = '0 0 0 2px rgba(79, 172, 254, 0.5)';
    }

    window.currentLogCategory = category;

    // 清空来源按钮状态
    document.querySelectorAll('.log-source-btn').forEach(btn => {
        btn.style.opacity = '0.6';
        btn.style.fontWeight = 'normal';
        btn.style.boxShadow = 'none';
    });
    window.currentLogSource = 'all';

    // 加载对应分类的日志
    loadLogsByCategory(category);
}

// 新增：切换日志来源
// 切换日志来源
function switchLogSource(source) {
    // 更新来源按钮高亮状态
    document.querySelectorAll('.log-source-btn').forEach(btn => {
        btn.style.opacity = '0.6';
        btn.style.fontWeight = 'normal';
        btn.style.boxShadow = 'none';
    });

    const btnMap = {
        'frontend': 'btnLogSourceFrontend',
        'api': 'btnLogSourceAPI',
        'worker': 'btnLogSourceWorker',
        'system': 'btnLogSourceSystem',
        'database': 'btnLogSourceDatabase',
        'websocket': 'btnLogSourceWebsocket'
    };

    const activeBtn = document.getElementById(btnMap[source]);
    if (activeBtn) {
        activeBtn.style.opacity = '1';
        activeBtn.style.fontWeight = 'bold';
        activeBtn.style.boxShadow = '0 0 0 2px rgba(79, 172, 254, 0.5)';
    }

    window.currentLogSource = source;

    // 清空分类按钮状态（除了全部按钮）
    document.querySelectorAll('.log-category-btn:not(#btnLogCategoryAll)').forEach(btn => {
        btn.style.opacity = '0.6';
        btn.style.fontWeight = 'normal';
        btn.style.boxShadow = 'none';
    });
    document.getElementById('btnLogCategoryAll').style.opacity = '1';
    document.getElementById('btnLogCategoryAll').style.fontWeight = 'bold';
    document.getElementById('btnLogCategoryAll').style.boxShadow = '0 0 0 2px rgba(79, 172, 254, 0.5)';
    window.currentLogCategory = 'all';

    // 加载对应来源的日志
    loadLogsByLevel(source);
}

// 切换日志类型（简化版：只支持 INFO 和 SYSTEM）
// 切换日志类型
function switchLogType(type) {
    // 更新下拉选择器的值
    const selector = document.getElementById('logTypeSelector');
    if (selector) {
        selector.value = type;
    }

    // 加载对应类型的日志
    loadLogsByType(type);
}

// 处理超级管理员响应
function handleSuperAdminResponse(msg) {
    // 隐藏加载遮罩
    hideLoading();

    // 检查是否是当前选中的服务器
    if (msg.server_id && msg.server_id !== currentSuperAdminServerId) {
        return; // 不是当前服务器的响应，忽略
    }

    // 显示响应消息
    if (msg.message) {
        appendWorkerConsole(msg.message, msg.success ? 'success' : 'error');
    }

    // 显示日志
    if (msg.logs && Array.isArray(msg.logs)) {
        msg.logs.forEach(log => {
            if (typeof log === 'string') {
                appendWorkerConsole(log, 'info');
            } else if (log.message) {
                appendWorkerConsole(log.message, log.type || 'info');
            }
        });
    }
}
//#endregion
//#region ==================== 系统页面路由与视图切换 ====================
// 处理用户登出
function handleLogout() {
    // 🔒 清除所有登录信息，包括所有token
    // 🔒 用户登录：1小时内自动登录，超过1小时需要重新输入密码
    if (typeof logoutAll === 'function') {
        logoutAll();
    }

    showLoginPage();
}

// 显示主应用界面
function showMainApp() {
    const loginPage = document.getElementById('loginPage');
    const adminPage = document.getElementById('adminPage');
    const managerPage = document.getElementById('managerPage');
    const contentWrapper = document.querySelector('.content-wrapper');
    const mainContainer = document.querySelector('.main-container');
    const panelA = document.getElementById('panelA');
    const panelB = document.getElementById('panelB');
    const panelC = document.getElementById('panelC');
    const panelD = document.getElementById('panelD');
    const panelE = document.getElementById('panelE');

    if (loginPage) {
        loginPage.style.display = 'none';
        document.body.classList.remove('login-mode');
    }
    if (adminPage) {
        adminPage.classList.remove('show');
        adminPage.style.display = 'none';
    }
    if (managerPage) {
        managerPage.style.display = 'none';
    }

    if (contentWrapper) {
        contentWrapper.style.display = 'flex';
    }
    if (mainContainer) {
        mainContainer.style.display = 'flex';
    }

    if (panelA) {
        panelA.style.display = 'flex';
    }
    if (panelB) {
        panelB.style.display = 'none';
        panelB.classList.remove('mobile-show');
    }
    if (panelC) {
        panelC.style.display = 'none';
    }
    if (panelD) {
        panelD.style.display = 'none';
    }
    if (panelE) {
        panelE.style.display = 'none';
        panelE.classList.remove('mobile-show');
    }

    const navHomeBtn = document.getElementById('navHomeBtn');
    const navAccountBtn = document.getElementById('navAccountBtn');
    const navSendBtn = document.getElementById('navSendBtn');
    const navInboxBtn = document.getElementById('navInboxBtn');
    if (navHomeBtn) navHomeBtn.classList.add('active');
    if (navAccountBtn) navAccountBtn.classList.remove('active');
    if (navSendBtn) navSendBtn.classList.remove('active');
    if (navInboxBtn) navInboxBtn.classList.remove('active');

}

// 显示登录页面
function showLoginPage() {
    const loginPage = document.getElementById('loginPage');
    const contentWrapper = document.querySelector('.content-wrapper');
    const mainContainer = document.querySelector('.main-container');
    const adminPage = document.getElementById('adminPage');
    const managerPage = document.getElementById('managerPage');

    if (loginPage) {
        loginPage.style.display = 'flex';
        document.body.classList.add('login-mode');
    }

    if (contentWrapper) contentWrapper.style.display = 'none';
    if (mainContainer) mainContainer.style.display = 'none';

    if (adminPage) {
        adminPage.classList.remove('show');
        adminPage.style.display = 'none';
    }
    if (managerPage) {
        managerPage.style.display = 'none';
    }
}

// 显示管理员页面
function showAdminPage() {
    document.getElementById('adminPage').classList.add('show');
    document.getElementById('loginPage').style.display = 'none';
    // 🔥 确保加载并显示服务器（立即加载，不等待）
    loadServersFromAPI().then(() => {
        // 延迟一下确保DOM已渲染
        setTimeout(() => {
            updateServerDisplay();
            // 初始化雷达机器人
            setTimeout(initRadarBots, 100);
        }, 100);
    }).catch(err => {
        // 即使加载失败，也尝试更新显示（可能使用本地数据）
        setTimeout(() => {
            updateServerDisplay();
            setTimeout(initRadarBots, 100);
        }, 100);
    });

    // 🔥 确保 WebSocket 连接已建立（用于接收实时服务器状态更新）
    if (!window.activeWs || window.activeWs.readyState !== WebSocket.OPEN) {
        // 如果没有 WebSocket 连接，尝试连接（服务器管理页面也需要实时更新）
        // 注意：这里不传 user_id，因为服务器管理页面可能不需要用户订阅
        setTimeout(() => {
            if (typeof connectToBackendWS === 'function') {
                connectToBackendWS(true); // 传入 true 表示忽略用户订阅
            }
        }, 500);
    }

    // 🔥 定期刷新服务器列表（每30秒）
    if (window.adminPageRefreshTimer) {
        clearInterval(window.adminPageRefreshTimer);
    }
    window.adminPageRefreshTimer = setInterval(() => {
        loadServersFromAPI().then(() => {
            updateServerDisplay();
        }).catch(() => { });
    }, 30000);
}

// 显示加载遮罩
function showLoading() {
    const el = document.getElementById('loadingOverlay');
    if (!el || !el.classList) return;
    el.classList.add('show');
}

// 隐藏加载遮罩
function hideLoading() {
    const el = document.getElementById('loadingOverlay');
    if (!el || !el.classList) return;
    el.classList.remove('show');
}

//#endregion
//#region ==================== 系统初始化入口 ====================
// 🔥 防止重复初始化
let _initPageExecuted = false;

// 页面初始化入口
async function initPage() {
    // 🔥 防止重复执行
    if (_initPageExecuted) {
        return;
    }
    _initPageExecuted = true;

    const loginPage = document.getElementById('loginPage');
    if (loginPage && loginPage.style.display !== 'none') {
        document.body.classList.add('login-mode');
    }

    // 🔥 首先检查管理员/服务器管理员是否已登录（使用sessionStorage）
    const adminToken = sessionStorage.getItem('admin_token');
    const serverManagerToken = sessionStorage.getItem('server_manager_token');

    // 🔥 如果管理员已登录，不进行普通用户的登录检查，直接返回
    if (adminToken || serverManagerToken) {
        // 管理员已登录，不自动跳转，让管理员继续使用
        return;
    }

    // 🔒 只处理普通用户“1小时后需重新输入账号密码”的前端门禁
    // 管理员/服务器管理/超级管理员：每次点击入口都必须弹密码框（与 token 无关），这里绝不自动放行
    const authToken = typeof getAuthToken === 'function' ? getAuthToken() : null;

    // 普通用户：基于token自动登录（服务端控制超时）
    if (authToken) {
        // 🔥 安全检查：确保 API_BASE_URL 已定义
        if (typeof API_BASE_URL === 'undefined' || !API_BASE_URL) {
            // API_BASE_URL未定义，可能是脚本加载顺序问题，显示登录页
            showLoginPage();
            return;
        }

        // 🔥 普通用户：1小时内直接进入（不校验 /verify，不删除 token）
        showMainApp();
        if (typeof window.init === 'function') {
            window.init();
        }
    } else {
        // 没有登录信息，显示登录页
        showLoginPage();
    }
}
//#endregion
//#region ==================== 雷达机器人动画逻辑 ====================
// 初始化雷达机器人到服务器按钮
// 初始化雷达机器人
function initRadarBots() {
    const buttons = document.querySelectorAll('.server-buttons-grid-new > button, .server-buttons-grid > button, .server-buttons-grid .server-button, .super-admin-servers-grid .super-admin-server-btn');
    const botHTML = SERVER_BOT_HTML;

    buttons.forEach(button => {
        if (!button.querySelector('.bot-container')) {
            button.innerHTML = botHTML + button.innerHTML;
        }
    });
}

// 使用MutationObserver监听按钮添加
// 监听服务器按钮
function observeServerButtons() {
    const observer = new MutationObserver((mutations) => {
        mutations.forEach((mutation) => {
            mutation.addedNodes.forEach((node) => {
                if (node.nodeType === 1) {
                    if (node.classList && (node.classList.contains('server-buttons-grid-new') || node.classList.contains('server-buttons-grid') || node.classList.contains('super-admin-servers-grid'))) {
                        initRadarBots();
                    } else if (node.querySelector && (node.querySelector('.server-buttons-grid-new') || node.querySelector('.server-buttons-grid') || node.querySelector('.super-admin-servers-grid'))) {
                        initRadarBots();
                    }
                }
            });
        });
    });

    const targetNode = document.body;
    observer.observe(targetNode, {
        childList: true,
        subtree: true
    });

    // 初始执行一次
    setTimeout(initRadarBots, 100);
}
//#endregion
//#region ==================== 页面关闭前数据保存与清理 ====================
// 页面关闭前保存管理员数据
// 保存管理员数据
function saveManagerDataBeforeUnload() {
    if (window.currentManagerId) {
        const account = window.adminAccounts.find(a => a.id === window.currentManagerId);
        if (account) {
            account.users = window.managerUsers;
            account.userGroups = window.managerUserGroups;
            if (typeof setAdminAccounts === 'function') {
                setAdminAccounts(window.adminAccounts);
            }
        }
    }
}

window.addEventListener('beforeunload', () => {
    saveManagerDataBeforeUnload();
    stopServerPolling();
    stopInboxPolling();
    stopAllTaskPolling();
});
//#endregion
//#region ==================== 启动时Token校验与自动登录 ====================
if (document.readyState === 'loading') {
    // 🔥 检查Token是否过期（7天有效期）
    async function checkTokenExpiry() {
        // 🔒 用户登录：1小时内自动登录，超过1小时需要重新输入密码
        // 检查登录时间是否超过1小时
        if (typeof isSessionTimeout === 'function' && isSessionTimeout()) {
            // 超过1小时：只清"登录时间"，强制重新输入账号密码；token 不删除
            if (typeof clearLoginTime === 'function') {
                clearLoginTime();
            }
            return;
        }
        // 注意：这里不做 /verify，不做任何 token 删除
    }

    document.addEventListener('DOMContentLoaded', async () => {
        // 🔥 防止重复初始化：检查是否已经执行过
        if (_initPageExecuted) {
            return;
        }
        // 页面加载时检查Token是否过期（initPage内部会验证，这里只做清理）
        checkTokenExpiry();
        // initPage现在会先验证token再决定是否登录
        await initPage();
        observeServerButtons();
    });
} else {
    // 🔥 防止重复初始化：检查是否已经执行过
    if (!_initPageExecuted) {
        initPage();
        observeServerButtons();
    }
}


//#endregion
//#region ==================== 服务器时间同步显示 ====================
// 更新服务器时间显示
function updateServerTime() {
    const now = new Date();
    // 使用芝加哥时区 (America/Chicago)
    const year = now.getFullYear();
    const month = String(now.getMonth() + 1).padStart(2, '0');
    const day = String(now.getDate()).padStart(2, '0');
    const hours = String(now.getHours()).padStart(2, '0');
    const minutes = String(now.getMinutes()).padStart(2, '0');

    const display = `${year}/${month}/${day} ${hours}:${minutes}`;

    // 更新服务器管理页面时间（新样式：分别更新日期和时间）
    const serverTimeDateEl = document.getElementById('serverTimeDate');
    const serverTimeClockEl = document.getElementById('serverTimeClock');
    if (serverTimeDateEl) serverTimeDateEl.textContent = `${year}/${month}/${day}`;
    if (serverTimeClockEl) serverTimeClockEl.textContent = `${hours}:${minutes}`;

    // 更新管理员页面时间（新样式：分别更新日期和时间）
    const managerTimeDateEl = document.getElementById('managerTimeDate');
    const managerTimeClockEl = document.getElementById('managerTimeClock');
    if (managerTimeDateEl) managerTimeDateEl.textContent = `${year}/${month}/${day}`;
    if (managerTimeClockEl) managerTimeClockEl.textContent = `${hours}:${minutes}`;

    // 更新超级管理员面板时间
    const superAdminTimeDateEl = document.getElementById('superAdminTimeDate');
    const superAdminTimeClockEl = document.getElementById('superAdminTimeClock');
    if (superAdminTimeDateEl) superAdminTimeDateEl.textContent = `${year}/${month}/${day}`;
    if (superAdminTimeClockEl) superAdminTimeClockEl.textContent = `${hours}:${minutes}`;

    // 更新主页面时间
    const mainPageTimeDateEl = document.getElementById('mainPageTimeDate');
    const mainPageTimeClockEl = document.getElementById('mainPageTimeClock');
    if (mainPageTimeDateEl) mainPageTimeDateEl.textContent = `${year}/${month}/${day}`;
    if (mainPageTimeClockEl) mainPageTimeClockEl.textContent = `${hours}:${minutes}`;
}

// 每秒更新服务器时间
setInterval(updateServerTime, 1000);
updateServerTime(); // 立即执行一次
//#endregion
//#region ==================== 费率配置管理 ====================
// Global state to track manual edits (to stop auto-sync)
const saManualEdits = {
    global: { recv: false, fail: false, private: false },
    sales: { recv: false, fail: false, private: false },
    user: { recv: false, fail: false, private: false }
};

// 提前声明函数，确保在DOMContentLoaded之前就可用
// 加载所有费率设置
function saLoadAllSettings() {
    // 🔥 修复：只在超级管理员面板显示时才加载设置
    const superAdminPanel = document.getElementById('superAdminPanel');
    const serverManagerToken = sessionStorage.getItem('server_manager_token');

    // 只有在超级管理员面板显示且已登录时才加载
    if (superAdminPanel && superAdminPanel.style.display !== 'none' && serverManagerToken) {
        if (typeof saLoadGlobalRates === 'function') {
            saLoadGlobalRates();
        } else {
            setTimeout(() => {
                if (typeof saLoadGlobalRates === 'function') {
                    saLoadGlobalRates();
                }
            }, 100);
        }
    }
}

document.addEventListener('DOMContentLoaded', () => {
    // Initial Load
    if (typeof saLoadAllSettings === 'function') {
        saLoadAllSettings();
    }
    if (typeof saBindAutoSyncEvents === 'function') {
        saBindAutoSyncEvents();
    }
});
// --- Auto Sync Logic ---
// 绑定自动同步事件
function saBindAutoSyncEvents() {
    const bindSync = (prefix, type) => {
        const sendInput = document.getElementById(`${prefix}Send`);
        const recvInput = document.getElementById(`${prefix}Recv`);
        const failInput = document.getElementById(`${prefix}Fail`);
        // Private input intentionally ignored for sync
        if (!sendInput) return;
        // When "Send" changes -> ALWAYS Force Sync Recv and Fail
        sendInput.addEventListener('input', () => {
            const val = parseFloat(sendInput.value);
            if (isNaN(val)) return;
            // Recv = Send
            if (recvInput) {
                recvInput.value = val;
            }
            // Fail = 1/3 of Send
            if (failInput) {
                failInput.value = (val / 3).toFixed(4);
            }
        });
        // Manual edits to Recv/Fail/Private just happen naturally 
        // and do not need any special logic to "block" future syncs.
    };
    bindSync('saGlobal', 'global');
    bindSync('saSales', 'sales');
}
// --- 1. 全局费率 (Global Rates) ---
// --- 1. 全局费率 (Global Rates) ---
// 加载全局费率
async function saLoadGlobalRates() {
    try {
        const serverManagerToken = sessionStorage.getItem('server_manager_token');
        if (!serverManagerToken) {
            const superAdminPanel = document.getElementById('superAdminPanel');
            if (superAdminPanel && superAdminPanel.style.display !== 'none') {
                appendSuperAdminLog('未通过密码验证，请重新输入密码', 'error');
            }
            return;
        }

        const res = await fetch(`${API_BASE_URL}/admin/rates/global`, {
            headers: {
                'Authorization': `Bearer ${serverManagerToken}`,
                'Content-Type': 'application/json'
            }
        });

        if (!res.ok) {
            const errorText = await res.text();
            throw new Error(`HTTP ${res.status}: ${errorText}`);
        }

        const data = await res.json();

        if (data.success && data.rates) {
            const r = data.rates;
            if (document.getElementById('saGlobalSend')) document.getElementById('saGlobalSend').value = r.send || '-';
            if (document.getElementById('saGlobalRecv')) document.getElementById('saGlobalRecv').value = r.recv || '-';
            if (document.getElementById('saGlobalFail')) document.getElementById('saGlobalFail').value = r.fail || '-';
            if (document.getElementById('saGlobalPrivate')) document.getElementById('saGlobalPrivate').value = r.private || '-';

            if (typeof setSaGlobalSend === 'function') {
                setSaGlobalSend(r.send || '0.00');
            }
            if (typeof setSaRatesGlobal === 'function') {
                setSaRatesGlobal(r);
            }
        } else {
            saShowToast('❌ 获取全局费率失败: ' + (data.message || '未知错误'), 'error');
        }
    } catch (e) {
        saShowToast('❌ 加载全局费率失败: ' + e.message, 'error');

        const cached = typeof getSaRatesGlobal === 'function' ? getSaRatesGlobal() : null;
        if (cached) {
            try {
                const r = cached;
                if (document.getElementById('saGlobalSend')) document.getElementById('saGlobalSend').value = r.send || '-';
                if (document.getElementById('saGlobalRecv')) document.getElementById('saGlobalRecv').value = r.recv || '-';
                if (document.getElementById('saGlobalFail')) document.getElementById('saGlobalFail').value = r.fail || '-';
                if (document.getElementById('saGlobalPrivate')) document.getElementById('saGlobalPrivate').value = r.private || '-';

                saShowToast('✅ 使用本地缓存的费率数据', 'warning');
            } catch (err) { }
        }
    }
}

// 切换全局费率编辑
function saToggleGlobalEdit() {
    saGlobalEditing = !saGlobalEditing;

    const inputs = document.querySelectorAll('#saGlobalDisplay .num-input');
    const editBtn = document.getElementById('saGlobalEditBtn');
    const saveBtn = document.getElementById('saGlobalSaveBtn');
    const cancelBtn = document.getElementById('saGlobalCancelBtn');
    const resetBtn = document.getElementById('saGlobalResetBtn');

    inputs.forEach(input => {
        input.readOnly = !saGlobalEditing;

        if (saGlobalEditing) {
            input.classList.add('editing');
            if (input.value === '-') {
                input.value = '';
            }
        } else {
            input.classList.remove('editing');
            if (input.value === '') {
                input.value = '-';
            }
        }
    });

    if (saGlobalEditing) {
        editBtn.style.display = 'none';
        saveBtn.style.display = 'block';
        cancelBtn.style.display = 'block';
        resetBtn.style.display = 'block';
    } else {
        editBtn.style.display = 'block';
        saveBtn.style.display = 'none';
        cancelBtn.style.display = 'none';
        resetBtn.style.display = 'none';
    }
}

// 重置全局费率为默认值
function saResetGlobalToDefault() {
    document.getElementById('saGlobalSend').value = '1';
    document.getElementById('saGlobalRecv').value = '1';
    document.getElementById('saGlobalFail').value = '1';
    document.getElementById('saGlobalPrivate').value = '1';
    saShowToast('全局费率已重置为默认值 1', 'success');
}

// 取消全局费率编辑
function saCancelGlobal() {
    saGlobalEditing = false;
    const inputs = document.querySelectorAll('#saGlobalDisplay .num-input');
    const editBtn = document.getElementById('saGlobalEditBtn');
    const saveBtn = document.getElementById('saGlobalSaveBtn');
    const cancelBtn = document.getElementById('saGlobalCancelBtn');
    const resetBtn = document.getElementById('saGlobalResetBtn');

    inputs.forEach(input => {
        input.readOnly = true;
        input.classList.remove('editing');
        if (input.value === '') {
            input.value = '-';
        }
    });

    editBtn.style.display = 'block';
    saveBtn.style.display = 'none';
    cancelBtn.style.display = 'none';
    resetBtn.style.display = 'none';

    saLoadGlobalRates();
}

// 保存全局费率
async function saSaveGlobal() {
    const rates = {
        send: document.getElementById('saGlobalSend').value,
        recv: document.getElementById('saGlobalRecv').value,
        fail: document.getElementById('saGlobalFail').value,
        private: document.getElementById('saGlobalPrivate').value
    };

    // 检查是否有值需要保存
    if ((rates.send === '-' || rates.send === '') &&
        (rates.recv === '-' || rates.recv === '') &&
        (rates.fail === '-' || rates.fail === '') &&
        (rates.private === '-' || rates.private === '')) {
        return; // 没有需要保存的数据
    }

    // 将空值转换为 '-'
    rates.send = rates.send === '' ? '-' : rates.send;
    rates.recv = rates.recv === '' ? '-' : rates.recv;
    rates.fail = rates.fail === '' ? '-' : rates.fail;
    rates.private = rates.private === '' ? '-' : rates.private;

    try {
        const serverManagerToken = sessionStorage.getItem('server_manager_token');
        if (!serverManagerToken) {
            saShowToast('❌ 未通过密码验证，请重新输入密码', 'error');
            return;
        }

        const res = await fetch(`${API_BASE_URL}/admin/rates/global`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${serverManagerToken}`
            },
            body: JSON.stringify({ rates })
        });
        const data = await res.json();
        if (data.success) {
            saShowToast('✅ 全局费率已保存 (Global Rates Saved)', 'success');
            await saLoadGlobalRates();
            saGlobalEditing = false;
            const editBtn = document.getElementById('saGlobalEditBtn');
            const saveBtn = document.getElementById('saGlobalSaveBtn');
            const cancelBtn = document.getElementById('saGlobalCancelBtn');
            const resetBtn = document.getElementById('saGlobalResetBtn');
            editBtn.style.display = 'block';
            saveBtn.style.display = 'none';
            cancelBtn.style.display = 'none';
            resetBtn.style.display = 'none';
            const inputs = document.querySelectorAll('#saGlobalDisplay .num-input');
            inputs.forEach(input => {
                input.readOnly = true;
                input.classList.remove('editing');
            });
        } else {
            saShowToast('❌ 保存失败: ' + (data.message || '未知错误'), 'error');
        }
    } catch (e) {
        saShowToast('❌ 网络错误: ' + e.message, 'error');
    }
}

// --- 2. 管理员费率范围设置 (Admin Rate Range) ---

// 打开管理员费率设置
function saToggleSalesSetting() {
    const inputArea = document.getElementById('saSalesInputArea');
    inputArea.style.display = 'block';
}

// 验证管理员账号
async function saVerifySalesperson() {
    const input = document.getElementById('saSalesSearchUser');
    const settingArea = document.getElementById('saSalesSettingArea');

    if (!input || !input.value.trim()) {
        saShowToast('请输入管理员ID或用户名', 'warning');
        return;
    }

    if (!settingArea) {
        return;
    }

    const inputValue = input.value.trim();

    try {
        const serverManagerToken = sessionStorage.getItem('server_manager_token');
        if (!serverManagerToken) {
            saShowToast('未通过密码验证，请重新输入密码', 'error');
            return;
        }

        const res = await fetch(`${API_BASE_URL}/admin/rates/admin-range?admin_id=${inputValue}`, {
            headers: {
                'Authorization': `Bearer ${serverManagerToken}`,
                'Content-Type': 'application/json'
            }
        });

        if (!res.ok) {
            if (res.status === 404) {
                saShowToast('管理员不存在', 'error');
            } else {
                const errorText = await res.text();
                saShowToast(`验证失败: HTTP ${res.status}`, 'error');
            }
            settingArea.style.display = 'none';
            return;
        }

        const data = await res.json();

        if (!data.success) {
            saShowToast(data.message || '管理员不存在', 'error');
            settingArea.style.display = 'none';
            return;
        }

        // 保存验证通过的管理员ID
        saCurrentAdminId = inputValue;
        settingArea.style.display = 'block';
        settingArea.classList.add('show');

        saSafeSetText('saSalesAdminId', inputValue);
        saSafeSetText('saSalesUserCount', data.user_count || 0);
        saSafeSetText('saSalesPerformance', data.performance || 0);

        if (data.rate_range) {
            document.getElementById('saSalesRangeMin').value = data.rate_range.min || '';
            document.getElementById('saSalesRangeMax').value = data.rate_range.max || '';
        } else {
            document.getElementById('saSalesRangeMin').value = '';
            document.getElementById('saSalesRangeMax').value = '';
        }

    } catch (e) {
        saShowToast('验证失败: ' + e.message, 'error');
        settingArea.style.display = 'none';
    }
}

// 保存管理员费率
async function saSaveSales() {
    if (!saCurrentAdminId) {
        saShowToast('❌ 请先验证管理员', 'error');
        return;
    }

    const minRate = parseFloat(document.getElementById('saSalesRangeMin').value);
    const maxRate = parseFloat(document.getElementById('saSalesRangeMax').value);

    if (isNaN(minRate) || isNaN(maxRate)) {
        saShowToast('❌ 请输入有效的费率范围（数字）', 'error');
        return;
    }

    if (minRate < 0.0001) {
        saShowToast('❌ 最小费率不能小于0.0001', 'error');
        return;
    }

    if (maxRate < minRate) {
        saShowToast('❌ 最大费率不能小于最小费率', 'error');
        return;
    }

    try {
        // 🔑 使用服务器管理 token
        const serverManagerToken = sessionStorage.getItem('server_manager_token');
        if (!serverManagerToken) {
            saShowToast('❌ 未通过密码验证，请重新输入密码', 'error');
            return;
        }

        const res = await fetch(`${API_BASE_URL}/admin/rates/admin-range`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${serverManagerToken}`
            },
            body: JSON.stringify({
                admin_id: saCurrentAdminId,
                rate_range: { min: minRate, max: maxRate }
            })
        });

        const data = await res.json();
        if (data.success) {
            saShowToast(`✅ 管理员 [${saCurrentAdminId}] 费率范围已保存`, 'success');
            // 隐藏所有设置相关元素，回到初始状态
            document.getElementById('saSalesInputArea').style.display = 'none';
            const settingArea = document.getElementById('saSalesSettingArea');
            settingArea.style.display = 'none';
            settingArea.classList.remove('show');
            document.getElementById('saSalesSearchUser').value = '';
            saCurrentAdminId = null;
            saResetSales();
            saLoadAllAdmins();
        } else {
            saShowToast('❌ 保存失败: ' + (data.message || '未知错误'), 'error');
        }
    } catch (e) {
        saShowToast('❌ 网络错误: ' + e.message, 'error');
    }
}

// 重置管理员费率区域
function saResetSales() {
    document.getElementById('saSalesRangeMin').value = '';
    document.getElementById('saSalesRangeMax').value = '';
}

let saRateConfirmResolve = null;

// 显示费率确认弹窗
function saShowRateConfirm(title, message) {
    const ratesSection = document.getElementById('superAdminRatesSection');
    if (!ratesSection) return Promise.resolve(false);

    let modalOverlay = document.getElementById('saRateConfirmOverlay');
    if (!modalOverlay) {
        modalOverlay = document.createElement('div');
        modalOverlay.id = 'saRateConfirmOverlay';
        modalOverlay.className = 'sa-rate-confirm-overlay';
        modalOverlay.innerHTML = `
            <div class="sa-rate-confirm-panel">
                <div class="sa-rate-confirm-header">
                    <span class="sa-rate-confirm-title" id="saRateConfirmTitle"></span>
                    <button class="sa-rate-confirm-close" onclick="saCloseRateConfirm(false)">×</button>
                </div>
                <div class="sa-rate-confirm-content">
                    <div class="sa-rate-confirm-message" id="saRateConfirmMessage"></div>
                    <div class="sa-rate-confirm-buttons">
                        <button class="sa-rate-confirm-btn cancel" onclick="saCloseRateConfirm(false)">取消</button>
                        <button class="sa-rate-confirm-btn confirm" onclick="saCloseRateConfirm(true)">确定</button>
                    </div>
                </div>
            </div>
        `;
        ratesSection.appendChild(modalOverlay);
    }

    saSafeSetText('saRateConfirmTitle', title);
    document.getElementById('saRateConfirmMessage').innerHTML = message;

    modalOverlay.style.display = 'flex';
    requestAnimationFrame(() => {
        modalOverlay.classList.add('show');
    });

    return new Promise((resolve) => {
        saRateConfirmResolve = resolve;
    });
}

// 关闭费率确认弹窗
function saCloseRateConfirm(result) {
    const modalOverlay = document.getElementById('saRateConfirmOverlay');
    if (!modalOverlay) return;

    modalOverlay.classList.remove('show');
    setTimeout(() => {
        modalOverlay.style.display = 'none';
    }, 200);

    if (saRateConfirmResolve) {
        saRateConfirmResolve(result);
        saRateConfirmResolve = null;
    }
}

// 重置管理员费率
async function saResetSalesRate() {
    if (!saCurrentAdminId) {
        saShowToast('请先验证管理员', 'warning');
        return;
    }

    const confirmed = await saShowRateConfirm(
        '确认重置',
        `确定要移除管理员 [${saCurrentAdminId}] 的费率范围设置吗？<br><span style="color: #ffd93d; font-size: 12px;">移除后该管理员将无法为用户设置自定义费率。</span>`
    );

    if (!confirmed) {
        return;
    }

    try {
        const serverManagerToken = sessionStorage.getItem('server_manager_token');
        if (!serverManagerToken) {
            saShowToast('未通过密码验证，请重新输入密码', 'error');
            return;
        }

        const res = await fetch(`${API_BASE_URL}/admin/rates/admin-range`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${serverManagerToken}`
            },
            body: JSON.stringify({
                admin_id: saCurrentAdminId,
                rate_range: null
            })
        });

        const data = await res.json();
        if (data.success) {
            saShowToast(`管理员 [${saCurrentAdminId}] 费率范围已重置`, 'success');
            // 隐藏所有设置相关元素，回到初始状态
            document.getElementById('saSalesInputArea').style.display = 'none';
            document.getElementById('saSalesSettingArea').style.display = 'none';
            document.getElementById('saSalesSearchUser').value = '';
            saCurrentAdminId = null;
            saResetSales();
            if (typeof saLoadAllAdmins === 'function') {
                saLoadAllAdmins();
            }
        } else {
            saShowToast('重置失败: ' + (data.message || '未知错误'), 'error');
        }
    } catch (e) {
        saShowToast('网络错误: ' + e.message, 'error');
    }
}

// 取消管理员费率编辑 - 关闭并隐藏所有设置相关元素
function saCancelSales() {
    document.getElementById('saSalesInputArea').style.display = 'none';
    const settingArea = document.getElementById('saSalesSettingArea');
    settingArea.style.display = 'none';
    settingArea.classList.remove('show');
    document.getElementById('saSalesSearchUser').value = '';
    saCurrentAdminId = null;
    saResetSales();
}


// --- 3. 指定用户费率 (Target User Rates) ---

// 打开用户费率设置
function saToggleUserSetting() {
    const inputArea = document.getElementById('saUserInputArea');
    inputArea.style.display = 'block';
}

// 验证用户账号
async function saVerifyUser() {
    const input = document.getElementById('saUserSearchName');
    const settingArea = document.getElementById('saUserSettingArea');

    if (!input || !input.value.trim()) {
        saShowToast('请输入用户ID或用户名', 'warning');
        return;
    }

    if (!settingArea) {
        return;
    }

    const userId = input.value.trim();

    try {
        const serverManagerToken = sessionStorage.getItem('server_manager_token');
        if (!serverManagerToken) {
            saShowToast('未通过密码验证，请重新输入密码', 'error');
            settingArea.style.display = 'none';
            return;
        }

        const userCheckResp = await fetch(`${API_BASE_URL}/user/${encodeURIComponent(userId)}/statistics`, {
            headers: {
                'Authorization': `Bearer ${serverManagerToken}`,
                'Content-Type': 'application/json'
            }
        });

        if (!userCheckResp.ok) {
            if (userCheckResp.status === 404) {
                saShowToast('用户不存在', 'error');
            } else {
                const errorText = await userCheckResp.text();
                saShowToast(`验证用户失败: HTTP ${userCheckResp.status}`, 'error');
            }
            settingArea.style.display = 'none';
            return;
        }

        const userData = await userCheckResp.json();

        // 使用API返回的真实user_id，而不是输入值
        saCurrentUserId = userData.user_id || userId;
        const username = userData.username || userId;
        const credits = parseFloat(userData.credits) || 0;

        let sendCount = 0;
        let totalRecharge = 0;
        let totalSpent = 0;

        if (userData.usage && Array.isArray(userData.usage)) {
            userData.usage.forEach(item => {
                if (item.action === 'send') {
                    sendCount++;
                } else if (item.action === 'recharge') {
                    totalRecharge += parseFloat(item.amount) || 0;
                }
            });
        }

        // 历史消费 = 总充值 - 当前余额
        totalSpent = totalRecharge - credits;
        if (totalSpent < 0) totalSpent = 0;

        // 显示用户基本信息
        saSafeSetText('saUserInfoName', username);
        saSafeSetText('saUserInfoId', saCurrentUserId);
        saSafeSetText('saUserInfoCredits', credits.toFixed(2));
        saSafeSetText('saUserInfoSendCount', sendCount);
        saSafeSetText('saUserInfoTotalRecharge', totalRecharge.toFixed(2));
        saSafeSetText('saUserInfoTotalSpent', totalSpent.toFixed(2));

        // 显示设置区域
        settingArea.style.display = 'block';
        settingArea.classList.add('show');

        // 获取用户费率（即使失败也不影响显示用户信息）
        try {
            const userRates = await fetchUserRates(saCurrentUserId);
            if (userRates) {
                // 显示当前费率
                saSafeSetText('saUserCurrentSend', userRates.send || '-');
                saSafeSetText('saUserCurrentRecv', userRates.recv || '-');
                saSafeSetText('saUserCurrentFail', userRates.fail || '-');
                saSafeSetText('saUserCurrentPrivate', userRates.private || '-');
                // 设置输入框默认值
                document.getElementById('saUserSend').value = userRates.send || '';
                document.getElementById('saUserRecv').value = userRates.recv || '';
                document.getElementById('saUserFail').value = userRates.fail || '';
                document.getElementById('saUserPrivate').value = userRates.private || '';
            } else {
                // 没有自定义费率，显示全局费率或默认
                saSafeSetText('saUserCurrentSend', '全局');
                saSafeSetText('saUserCurrentRecv', '全局');
                saSafeSetText('saUserCurrentFail', '全局');
                saSafeSetText('saUserCurrentPrivate', '全局');
                document.getElementById('saUserSend').value = '';
                document.getElementById('saUserRecv').value = '';
                document.getElementById('saUserFail').value = '';
                document.getElementById('saUserPrivate').value = '';
            }
        } catch (rateError) {
        }

        const inputs = document.querySelectorAll('#saUserSettingArea .num-input');
        inputs.forEach(input => {
            input.readOnly = false;
            input.classList.add('editing');
        });

    } catch (e) {
        saShowToast('验证用户失败: ' + e.message, 'error');
        settingArea.style.display = 'none';
    }
}

// 获取用户费率
async function fetchUserRates(userId) {
    try {
        const serverManagerToken = sessionStorage.getItem('server_manager_token');
        if (!serverManagerToken) {
            return null;
        }

        const res = await fetch(`${API_BASE_URL}/admin/rates/user?user_id=${userId}`, {
            headers: {
                'Authorization': `Bearer ${serverManagerToken}`
            }
        });

        const data = await res.json();

        if (data.success && data.rates) {
            return data.rates;
        }
        return null;
    } catch (e) {
        return null;
    }
}

// 保存用户费率
async function saSaveUser() {
    if (!saCurrentUserId) {
        saShowToast('❌ 请先验证用户', 'error');
        return;
    }

    const rates = {
        send: document.getElementById('saUserSend').value,
        recv: document.getElementById('saUserRecv').value,
        fail: document.getElementById('saUserFail').value,
        private: document.getElementById('saUserPrivate').value
    };

    // 过滤空值和 "-"
    const cleanRates = {};
    if (rates.send && rates.send !== '-' && rates.send !== '') cleanRates.send = rates.send;
    if (rates.recv && rates.recv !== '-' && rates.recv !== '') cleanRates.recv = rates.recv;
    if (rates.fail && rates.fail !== '-' && rates.fail !== '') cleanRates.fail = rates.fail;
    if (rates.private && rates.private !== '-' && rates.private !== '') cleanRates.private = rates.private;

    // 如果全空，询问是否清除
    if (Object.keys(cleanRates).length === 0) {
        if (!confirm("未输入任何费率，这将清除该用户的自定义费率设置（恢复使用全局费率）。确定吗？")) return;
    }

    try {
        const serverManagerToken = sessionStorage.getItem('server_manager_token');
        if (!serverManagerToken) {
            saShowToast('❌ 未通过密码验证，请重新输入密码', 'error');
            return;
        }

        const res = await fetch(`${API_BASE_URL}/admin/rates/user`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${serverManagerToken}`
            },
            body: JSON.stringify({
                user_id: saCurrentUserId,
                rates: Object.keys(cleanRates).length > 0 ? cleanRates : null
            })
        });

        if (!res.ok) {
            const errorText = await res.text();
            throw new Error(`HTTP ${res.status}: ${errorText}`);
        }

        const data = await res.json();
        if (data.success) {
            saShowToast(`✅ 用户 [${saCurrentUserId}] 费率已保存`, 'success');
            // 隐藏所有设置相关元素，回到初始状态
            document.getElementById('saUserInputArea').style.display = 'none';
            const userSettingArea = document.getElementById('saUserSettingArea');
            userSettingArea.style.display = 'none';
            userSettingArea.classList.remove('show');
            document.getElementById('saUserSearchName').value = '';
            saCurrentUserId = null;
            saResetUser();
            if (typeof saLoadAllUsers === 'function') {
                saLoadAllUsers();
            }
        } else {
            saShowToast('❌ 保存失败: ' + (data.message || '未知错误'), 'error');
        }
    } catch (e) {
        saShowToast('❌ 网络错误: ' + e.message, 'error');
    }
}

// 重置用户费率区域
function saResetUser() {
    document.getElementById('saUserSend').value = '-';
    document.getElementById('saUserRecv').value = '-';
    document.getElementById('saUserFail').value = '-';
    document.getElementById('saUserPrivate').value = '-';
}

// 重置用户费率
async function saResetUserRate() {
    if (!saCurrentUserId) {
        saShowToast('请先验证用户', 'warning');
        return;
    }

    const confirmed = await saShowRateConfirm(
        '确认重置',
        `确定要移除用户 [${saCurrentUserId}] 的自定义费率设置吗？<br><span style="color: #ffd93d; font-size: 12px;">移除后该用户将使用服务器全局费率。</span>`
    );

    if (!confirmed) {
        return;
    }

    try {
        const serverManagerToken = sessionStorage.getItem('server_manager_token');
        if (!serverManagerToken) {
            saShowToast('未通过密码验证，请重新输入密码', 'error');
            return;
        }

        const res = await fetch(`${API_BASE_URL}/admin/rates/user`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${serverManagerToken}`
            },
            body: JSON.stringify({
                user_id: saCurrentUserId,
                rates: null
            })
        });

        const data = await res.json();
        if (data.success) {
            saShowToast(`用户 [${saCurrentUserId}] 费率已重置为全局费率`, 'success');
            // 隐藏所有设置相关元素，回到初始状态
            document.getElementById('saUserInputArea').style.display = 'none';
            document.getElementById('saUserSettingArea').style.display = 'none';
            document.getElementById('saUserSearchName').value = '';
            saCurrentUserId = null;
            saResetUser();
            if (typeof saLoadAllUsers === 'function') {
                saLoadAllUsers();
            }
        } else {
            saShowToast('重置失败: ' + (data.message || '未知错误'), 'error');
        }
    } catch (e) {
        saShowToast('网络错误: ' + e.message, 'error');
    }
}

// 取消用户费率编辑 - 关闭并隐藏所有设置相关元素
function saCancelUser() {
    document.getElementById('saUserInputArea').style.display = 'none';
    const userSettingArea = document.getElementById('saUserSettingArea');
    userSettingArea.style.display = 'none';
    userSettingArea.classList.remove('show');
    document.getElementById('saUserSearchName').value = '';
    saCurrentUserId = null;
    saResetUser();
}



//#endregion
//#region ==================== 数据管理 ====================

let saAllUsersData = [];
let saAllAdminsData = [];

// 切换数据Tab
async function saSwitchDataTab(tab) {
    // 更新按钮状态
    ['user', 'admin', 'server'].forEach(t => {
        const btn = document.getElementById('btnSaData' + t.charAt(0).toUpperCase() + t.slice(1));
        if (btn) {
            btn.classList.toggle('active', t === tab);
        }
    });

    // 更新面板显示
    document.getElementById('saDataUserPanel').style.display = tab === 'user' ? 'block' : 'none';
    document.getElementById('saDataAdminPanel').style.display = tab === 'admin' ? 'block' : 'none';
    document.getElementById('saDataServerPanel').style.display = tab === 'server' ? 'block' : 'none';

    // 加载数据
    if (tab === 'user') {
        await saLoadAllUsers();
    } else if (tab === 'admin') {
        await saLoadAllAdmins();
    } else if (tab === 'server') {
        await saLoadServerData();
    }
}
// 加载服务器统计数据
async function saLoadServerData() {
    const panel = document.getElementById('saDataServerPanel');
    if (!panel) return;

    try {
        const token = sessionStorage.getItem('server_manager_token') || '';
        if (!token) {
            appendSuperAdminLog('未登录或会话已过期', 'error');
            saRenderServerData({});
            return;
        }

        let statsData = {};

        try {
            const statsResponse = await fetch(`${API_BASE_URL}/server_manager/stats`, {
                headers: { 'Authorization': `Bearer ${token}` }
            });
            if (statsResponse.ok) {
                const statsJson = await statsResponse.json();
                if (statsJson.ok || statsJson.success) {
                    statsData = statsJson.data || statsJson;
                }
            }
        } catch (e) {
            console.log('Stats API not available:', e.message);
        }

        const serversResponse = await fetch(`${API_BASE_URL}/servers?t=${Date.now()}`, {
            headers: { 'Authorization': `Bearer ${token}` }
        });

        let serversData = { servers: [] };
        if (serversResponse.ok) {
            const serversJson = await serversResponse.json();
            if (serversJson.success && serversJson.servers) {
                serversData = serversJson;
            }
        }

        const now = Date.now();
        let connectedCount = 0;
        let disconnectedCount = 0;

        (serversData.servers || []).forEach(s => {
            if (isSuperAdminServerOnline(s, now)) {
                connectedCount++;
            } else {
                disconnectedCount++;
            }
        });

        const combinedData = {
            ...statsData,
            available_servers: connectedCount,
            server_current: connectedCount,
            server_disconnected: disconnectedCount,
            server_history: (serversData.servers || []).length,
            status: statsData.status || 'RUNNING',
            online_users: statsData.online_users || 0,
            start_time: statsData.start_time || statsData.server_start_time || '-',
            uptime: statsData.uptime || '-',
            error_logs: statsData.error_logs || 0,
            new_users: statsData.new_users || 0,
            total_income: statsData.total_income || 0,
            stats: statsData.stats || [],
            recharge_today: statsData.recharge_today || 0,
            recharge_3days: statsData.recharge_3days || 0,
            recharge_month: statsData.recharge_month || 0,
            recharge_total: statsData.recharge_total || 0,
            send_total: statsData.send_total || 0,
            bill_total: statsData.bill_total || 0
        };

        saRenderServerData(combinedData);
    } catch (e) {
        console.error('Load server data error:', e);
        saRenderServerData({});
    }
}
// 渲染服务器统计数据
function saRenderServerData(data) {
    try {
    const setText = (id, value) => {
        const el = document.getElementById(id);
        if (el) el.textContent = value;
    };
    const formatShortTime = (value) => {
        if (!value) return '-';
        const txt = String(value).trim();
        const m = txt.match(/(\d{1,2})[\/\-](\d{1,2})[\/\-]?\d*\s+(\d{1,2}:\d{2})/);
        if (m) return `${m[1]}/${m[2]} ${m[3]}`;
        const d = new Date(txt);
        if (Number.isNaN(d.getTime())) return txt;
        const mm = d.getMonth() + 1;
        const dd = d.getDate();
        const hh = String(d.getHours()).padStart(2, '0');
        const mi = String(d.getMinutes()).padStart(2, '0');
        return `${mm}/${dd} ${hh}:${mi}`;
    };

    const isOnlineStatusLocal = (status) => {
        if (typeof isOnlineStatus === 'function') return isOnlineStatus(status);
        const s = String(status || '').toLowerCase();
        return s === 'ok' || s === 'connected' || s === 'online' || s === 'available' || s === 'ready';
    };

    const saServerStatusEl = document.getElementById('saServerStatus');
    if (saServerStatusEl) {
        saServerStatusEl.textContent = data.status || 'RUNNING';
        saServerStatusEl.className = `value ${data.status === 'RUNNING' ? 'running' : 'error'}`;
    }

    setText('saServerStartTime', data.start_time || '-');
    setText('saServerUptime', data.uptime || '-');
    setText('saErrorLogs', data.error_logs || 0);

    setText('saOnlineUsers', data.online_users || 0);
    setText('saAvailableServers', data.available_servers || 0);
    setText('saNewUsers', data.new_users || 0);
    setText('saTotalIncome', '$' + (data.total_income || 0));

    const statsRows = document.getElementById('saStatsRows');
    if (statsRows && data.stats) {
        const days = ['今天:', '昨天:', '前天:'];
        statsRows.innerHTML = days.map((day, i) => {
            const s = data.stats[i] || {};
            return `<tr>
                <td>${day}</td>
                <td>${s.send || 0}</td>
                <td>${s.recv || 0}</td>
                <td>${s.rate || '0%'}</td>
                <td>${s.reg || 0}</td>
                <td>${s.visit || 0}</td>
                <td>${s.consume || 0}</td>
                <td>${s.income || 0}</td>
                <td>${s.total_income || 0}</td>
            </tr>`;
        }).join('');
    }

    const rechargeRecordsRaw = Array.isArray(data.recharge_records) ? data.recharge_records : [];
    const sendRecordsRaw = Array.isArray(data.send_records) ? data.send_records : [];

    setText('saRechargeToday', data.recharge_today || 0);
    setText('saRecharge3Days', data.recharge_3days || 0);
    setText('saRechargeMonth', data.recharge_month || 0);
    setText('saRechargeTotal', data.recharge_total || 0);
    setText('saRechargeAllCount', data.recharge_all_count || rechargeRecordsRaw.length);

    const sendTotal = Number(data.send_total || sendRecordsRaw.reduce((sum, r) => sum + Number(r.send || 0), 0));
    const recvTotal = Number(data.recv_total || sendRecordsRaw.reduce((sum, r) => sum + Number(r.recv || 0), 0));
    const billTotal = Number(data.bill_total || sendRecordsRaw.reduce((sum, r) => {
        const send = Number(r.send || 0);
        const recv = Number(r.recv || 0);
        const fail = Number(r.fail || 0);
        const includeFail = Boolean(r.include_fail_in_bill);
        return sum + send + recv + (includeFail ? fail : 0);
    }, 0));
    setText('saSendSum', sendTotal);
    setText('saRecvSum', recvTotal);
    setText('saBillSum', billTotal.toFixed(2));

    setText('saServerHistoryTotal', data.server_history || 0);
    setText('saServerCurrent', data.server_current || 0);
    setText('saServerDisconnected', data.server_disconnected || 0);

    const userRateCount = data.rate_user_count || 0;
    const adminRateCount = data.rate_admin_count || 0;
    setText('saRateUserCount', userRateCount);
    setText('saRateAdminCount', adminRateCount);

    const rechargeList = document.getElementById('saRechargeRecords');
    if (rechargeList) {
        const records = rechargeRecordsRaw;
        rechargeList.innerHTML = records.slice(0, 10).map((r, index) => {
            const time = formatShortTime(r.time);
            const userId = r.user_id || r.username || '-';
            const amount = Number(r.amount || 0);
            const amountDisplay = amount >= 0 ? `+${amount.toFixed(2)}` : amount.toFixed(2);
            const amountColor = amount >= 0 ? '#00ff88' : '#ff4757';
            return `<div class="record-item recharge">
                <div class="record-line-1">
                    <span class="recharge-index">${index + 1}.</span>
                    <span class="recharge-time">${time}</span>
                </div>
                <div class="record-line-2">
                    <span class="recharge-user">ID:${userId}</span>
                    <span class="recharge-amount" style="color: ${amountColor};">${amountDisplay}</span>
                </div>
            </div>`;
        }).join('');
    }

    const sendList = document.getElementById('saSendRecords');
    if (sendList) {
        const records = sendRecordsRaw;
        sendList.innerHTML = records.slice(0, 10).map((r, index) => {
            const time = formatShortTime(r.time);
            const userId = r.user_id || r.username || '-';
            const send = Number(r.send || 0);
            const recv = Number(r.recv || 0);
            const fail = Number(r.fail || 0);
            const includeFail = Boolean(r.include_fail_in_bill);
            const bill = Number(r.bill ?? (send + recv + (includeFail ? fail : 0)));
            return `<div class="record-item send">
                <div class="record-line-1">
                    <span class="send-index">${index + 1}.</span>
                    <span class="send-time">${time}</span>
                    <span class="send-user">${userId}</span>
                </div>
                <div class="record-line-2 send-stats-inline">
                    <span class="send-total">发送:${send}</span>
                    <span class="send-recv">接收:${recv}</span>
                    <span class="send-cost">计费:${bill}</span>
                </div>
            </div>`;
        }).join('');
    }

    const rateList = document.getElementById('saRateRecords');
    if (rateList) {
        const records = Array.isArray(data.rate_records) ? data.rate_records : [];
        rateList.innerHTML = records.slice(0, 10).map((r, index) => {
            const time = formatShortTime(r.time);
            const isActiveRecord = Boolean(r.active || r.is_active || r.current || index === 0);
            const activeClass = isActiveRecord ? ' active' : '';
            if (r.type === 'user') {
                return `<div class="record-item rate user${activeClass}">
                    <div class="record-line-1">
                        <span class="rate-index">${index + 1}.</span>
                        <span class="rate-time">${time}</span>
                        <span class="rate-user-name">${r.target_user || '-'}</span>
                    </div>
                    <div class="record-line-2 rate-detail">
                        <span class="rate-send">发送:${r.new_user_rate || '0'}</span>
                        <span class="rate-recv">接收:${r.new_recv_rate || '0'}</span>
                        <span class="rate-fail">失败:${r.new_fail_rate || '0'}</span>
                        <span class="rate-private">私享:${r.new_private_rate || '0'}</span>
                    </div>
                </div>`;
            }
            return `<div class="record-item rate admin${activeClass}">
                <div class="record-line-1">
                    <span class="rate-index">${index + 1}.</span>
                    <span class="rate-time">${time}</span>
                    <span class="rate-admin-name">${r.target_admin || '-'}</span>
                </div>
                <div class="record-line-2 rate-detail">
                    <span class="rate-admin-range">费率权限: ${r.new_admin_rate || '-'}</span>
                </div>
            </div>`;
        }).join('');
    }

    const serverList = document.getElementById('saServerRecords');
    if (serverList) {
        const servers = Array.isArray(data.server_list) ? data.server_list : [];
        serverList.innerHTML = servers.map((s, index) => {
            const serverId = s.server_id || s.name || `server_${index + 1}`;
            const status = s.status || 'disconnected';
            const isOnline = isOnlineStatusLocal(status);
            const statusClass = isOnline ? 'online' : 'offline';
            const statusText = isOnline ? '正在运行' : '已断开';
            return `<div class="server-card">
                <span class="server-serial">${index + 1}.</span>
                <span class="server-pill ${statusClass}" title="${statusText}">${serverId}</span>
            </div>`;
        }).join('');
    }
    } catch (e) {
        console.error('saRenderServerData error:', e);
    }
}

async function saLoadAllAdmins() {
    const panel = document.getElementById('saDataAdminPanel');
    if (!panel) return;

    const container = document.getElementById('saAllAdminList');
    const countEl = document.getElementById('saTotalAdminCount');

    // Clear container
    if (container) container.innerHTML = '';
    if (countEl) countEl.textContent = '0';

    // 清空用户列表，避免显示混乱
    const userList = document.getElementById('saAllUserList');
    if (userList) userList.innerHTML = '';

    // 隐藏用户面板的搜索框
    const userSearchContainer = document.getElementById('saUserSearchContainer');
    if (userSearchContainer) userSearchContainer.style.display = 'none';

    try {
        const token = sessionStorage.getItem('server_manager_token') || '';
        if (!token) {
            appendSuperAdminLog('未登录或会话已过期', 'error');
            return;
        }

        // Fetch admin accounts
        const response = await fetch(`${API_BASE_URL}/admin/account`, {
            headers: { 'Authorization': `Bearer ${token}` }
        });

        if (response.ok) {
            const data = await response.json();
            if (data.success) {
                const admins = data.admins || [];
                // Store admin data for search functionality
                saAllAdminsData = admins;
                if (countEl) countEl.textContent = admins.length;
                saRenderAdminList(admins);
            }
        }

    } catch (e) {
    }
}
// 渲染管理员列表
function saRenderAdminList(admins) {
    const container = document.getElementById('saAllAdminList');
    if (!container) return;

    if (!admins || admins.length === 0) {
        container.innerHTML = '';
        return;
    }

    container.innerHTML = admins.map(a => {
        // API返回的是 admin_id 和 selected_servers（下划线格式）
        const username = a.admin_id || a.username || 'Admin';
        const uid = String(a.id || a.admin_id || '');
        const serverCount = Array.isArray(a.selected_servers) ? a.selected_servers.length : 0;
        const created = a.created ? new Date(a.created).toLocaleDateString('zh-CN') : '-';

        return `
        <div class="user-button sa-admin-card" data-admin-id="${uid}">
            <div class="user-button-content">
                <div class="user-server-count-badge ${serverCount > 0 ? 'flash' : ''}">${serverCount}</div>
                <div class="user-button-info">
                    <div class="user-button-top">
                        <span class="user-id-text">${username}</span>
                        <div style="display: flex; gap: 5px;">
                            <button class="sa-user-detail-btn" onclick="saViewAdminDetail('${uid}', '${username}')">详情</button>
                        </div>
                    </div>
                    <div class="user-button-stats">
                        <div class="user-stat-item">
                            <span class="user-stat-label">servers:</span>
                            <span class="user-stat-value">${serverCount}</span>
                        </div>
                        <div class="user-stat-item">
                            <span class="user-stat-label">created:</span>
                            <span class="user-stat-value">${created}</span>
                        </div>
                    </div>
                </div>
            </div>
        </div>
        `;
    }).join('');
}
// 加载所有用户
async function saLoadAllUsers() {
    const container = document.getElementById('saAllUserList');
    const countEl = document.getElementById('saTotalUserCount');
    if (!container) {
        return;
    }

    container.innerHTML = '';
    if (countEl) countEl.textContent = '0';

    // 清空管理员列表，避免显示混乱
    const adminList = document.getElementById('saAllAdminList');
    if (adminList) adminList.innerHTML = '';

    // 隐藏管理员面板的搜索框
    const adminSearchContainer = document.getElementById('saAdminSearchContainer');
    if (adminSearchContainer) adminSearchContainer.style.display = 'none';

    try {
        const token = sessionStorage.getItem('server_manager_token') || '';
        if (!token) {
            appendSuperAdminLog('未登录或会话已过期，请重新输入密码', 'error');
            return;
        }

        const response = await fetch(`${API_BASE_URL}/admin/users/all`, {
            headers: { 'Authorization': `Bearer ${token}` }
        });

        if (!response.ok) {
            const errorText = await response.text();
            return;
        }

        const data = await response.json();
        if (data.success) {
            saAllUsersData = data.users || [];
            if (countEl) countEl.textContent = data.total || saAllUsersData.length || 0;
            saRenderUserList(saAllUsersData);
        } else {
            appendSuperAdminLog('加载用户数据失败: ' + (data.message || '未知错误'), 'error');
        }
    } catch (e) {
        appendSuperAdminLog('加载用户数据异常: ' + e.message, 'error');
    }
}

// 用户搜索显示状态
let saUserSearchDisplay = 'none';

// 切换用户搜索
function saToggleUserSearch() {
    const container = document.getElementById('saUserSearchContainer');
    if (container) {
        saUserSearchDisplay = saUserSearchDisplay === 'none' ? 'flex' : 'none';
        container.style.display = saUserSearchDisplay;
        if (saUserSearchDisplay === 'flex') {
            document.getElementById('saUserSearchInput')?.focus();
        }
    }
}
// 确认用户搜索
function saConfirmUserSearch() {
    const input = document.getElementById('saUserSearchInput');
    if (!input) return;

    const keyword = input.value.trim();
    if (!keyword) {
        saRenderUserList(saAllUsersData);
        return;
    }

    // 精确匹配：匹配 user_id 或 username（完全相等）
    const foundUser = saAllUsersData.find(u =>
        String(u.user_id) === keyword ||
        (u.username && String(u.username) === keyword)
    );

    if (foundUser) {
        const userId = foundUser.user_id;
        const otherUsers = saAllUsersData.filter(u => u.user_id !== userId);
        // 将目标用户移到列表第一位
        saRenderUserList([foundUser, ...otherUsers]);
        document.getElementById('saUserSearchContainer').style.display = 'none';
        input.value = '';

        // 添加动画类给搜索结果
        setTimeout(() => {
            const firstCard = document.querySelector('.sa-user-card');
            if (firstCard) {
                firstCard.classList.add('move-up');
                setTimeout(() => firstCard.classList.remove('move-up'), 500);
            }
        }, 100);
    } else {
        saRenderUserList(saAllUsersData);
        document.getElementById('saUserSearchContainer').style.display = 'none';
        input.value = '';
    }
}
// 取消用户搜索
function saCancelUserSearch() {
    const container = document.getElementById('saUserSearchContainer');
    if (container) {
        container.style.display = 'none';
    }
    const input = document.getElementById('saUserSearchInput');
    if (input) {
        input.value = '';
    }
    const searchIcon = document.getElementById('saUserSearchIcon');
    if (searchIcon) {
        searchIcon.style.display = 'block';
    }
    const searchBtn = document.getElementById('saUserSearchBtn');
    if (searchBtn) {
        searchBtn.style.display = 'none';
    }
    saRenderUserList(saAllUsersData);
}
// 取消管理员搜索
function saCancelAdminSearch() {
    const container = document.getElementById('saAdminSearchContainer');
    if (container) {
        container.style.display = 'none';
    }
    const input = document.getElementById('saAdminSearchInput');
    if (input) {
        input.value = '';
    }
    const searchIcon = document.getElementById('saAdminSearchIcon');
    if (searchIcon) {
        searchIcon.style.display = 'block';
    }
    const searchBtn = document.getElementById('saAdminSearchBtn');
    if (searchBtn) {
        searchBtn.style.display = 'none';
    }
    saRenderAdminList(saAllAdminsData);
}
// 切换用户搜索
function saToggleUserSearch() {
    const container = document.getElementById('saUserSearchContainer');
    if (container) {
        const currentDisplay = window.getComputedStyle(container).display;
        const newDisplay = currentDisplay === 'none' ? 'flex' : 'none';
        container.style.display = newDisplay;
        if (newDisplay === 'flex') {
            const searchIcon = document.getElementById('saUserSearchIcon');
            if (searchIcon) {
                searchIcon.style.display = 'none';
            }
            const searchBtn = document.getElementById('saUserSearchBtn');
            if (searchBtn) {
                searchBtn.style.display = 'block';
            }
            document.getElementById('saUserSearchInput')?.focus();
        } else {
            saCancelUserSearch();
        }
    }
}
// 取消用户搜索
function saCancelUserSearch() {
    const container = document.getElementById('saUserSearchContainer');
    if (container) {
        container.style.display = 'none';
    }
    const input = document.getElementById('saUserSearchInput');
    if (input) {
        input.value = '';
    }
    const searchIcon = document.getElementById('saUserSearchIcon');
    if (searchIcon) {
        searchIcon.style.display = 'block';
    }
    const searchBtn = document.getElementById('saUserSearchBtn');
    if (searchBtn) {
        searchBtn.style.display = 'none';
    }
    saRenderUserList(saAllUsersData);
}
// 取消管理员搜索
function saCancelAdminSearch() {
    const container = document.getElementById('saAdminSearchContainer');
    if (container) {
        container.style.display = 'none';
    }
    const input = document.getElementById('saAdminSearchInput');
    if (input) {
        input.value = '';
    }
    const searchIcon = document.getElementById('saAdminSearchIcon');
    if (searchIcon) {
        searchIcon.style.display = 'block';
    }
    const searchBtn = document.getElementById('saAdminSearchBtn');
    if (searchBtn) {
        searchBtn.style.display = 'none';
    }
    saRenderAdminList(saAllAdminsData);
}
// 切换用户搜索
function saToggleUserSearch() {
    const container = document.getElementById('saUserSearchContainer');
    if (container) {
        const currentDisplay = window.getComputedStyle(container).display;
        const newDisplay = currentDisplay === 'none' ? 'flex' : 'none';
        container.style.display = newDisplay;
        if (newDisplay === 'flex') {
            const searchIcon = document.getElementById('saUserSearchIcon');
            if (searchIcon) {
                searchIcon.style.display = 'none';
            }
            const searchBtn = document.getElementById('saUserSearchBtn');
            if (searchBtn) {
                searchBtn.style.display = 'block';
            }
            document.getElementById('saUserSearchInput')?.focus();
        } else {
            saCancelUserSearch();
        }
    }
}
// 切换管理员搜索
function saToggleAdminSearch() {
    const container = document.getElementById('saAdminSearchContainer');
    if (container) {
        const currentDisplay = window.getComputedStyle(container).display;
        const newDisplay = currentDisplay === 'none' ? 'flex' : 'none';
        container.style.display = newDisplay;
        if (newDisplay === 'flex') {
            const searchIcon = document.getElementById('saAdminSearchIcon');
            if (searchIcon) {
                searchIcon.style.display = 'none';
            }
            const searchBtn = document.getElementById('saAdminSearchBtn');
            if (searchBtn) {
                searchBtn.style.display = 'block';
            }
            document.getElementById('saAdminSearchInput')?.focus();
        } else {
            saCancelAdminSearch();
        }
    }
}

// 确认管理员搜索
function saConfirmAdminSearch() {
    const input = document.getElementById('saAdminSearchInput');
    if (!input) return;

    const keyword = input.value.trim();
    if (!keyword) {
        saRenderAdminList(saAllAdminsData);
        return;
    }

    // 精确匹配：匹配 admin_id 或 username（完全相等）
    const foundAdmin = saAllAdminsData.find(a =>
        String(a.admin_id) === keyword ||
        (a.username && String(a.username) === keyword)
    );

    if (foundAdmin) {
        const adminId = String(foundAdmin.admin_id);
        const otherAdmins = saAllAdminsData.filter(a => String(a.admin_id) !== adminId);
        // 将目标管理员移到列表第一位
        saRenderAdminList([foundAdmin, ...otherAdmins]);
        document.getElementById('saAdminSearchContainer').style.display = 'none';
        input.value = '';

        // 添加动画类给搜索结果
        setTimeout(() => {
            const firstCard = document.querySelector('.sa-admin-card');
            if (firstCard) {
                firstCard.classList.add('move-up');
                setTimeout(() => firstCard.classList.remove('move-up'), 500);
            }
        }, 100);
    } else {
        saRenderAdminList(saAllAdminsData);
        document.getElementById('saAdminSearchContainer').style.display = 'none';
        input.value = '';
    }
}
// 渲染用户列表
function saRenderUserList(users) {
    const container = document.getElementById('saAllUserList');
    if (!container || !users || users.length === 0) {
        if (container) container.innerHTML = '';
        return;
    }

    container.innerHTML = users.map(u => {
        const username = u.username || '未设置';
        const uid = u.user_id;
        const fullUserId = String(uid);
        let userIdDisplay = fullUserId.startsWith('u_') ? fullUserId.substring(2) : fullUserId;
        const balance = (u.credits || 0).toFixed(2);
        const serverCount = u.server_count || 0;
        const globalSendRate = typeof getSaGlobalSend === 'function' ? getSaGlobalSend() : '0.00';
        const sendRate = u.send_rate || globalSendRate || '0.00';
        const escapedUserId = fullUserId.replace(/"/g, '&quot;').replace(/'/g, '&#39;');

        return `
                    <div class="user-button sa-user-card" data-user-id="${uid}">
                        <div class="user-button-content">
                            <div class="user-server-count-badge ${serverCount > 0 ? 'flash' : ''}">${serverCount}</div>
                            <div class="user-button-info">
                                <div class="user-button-top">
                                    <span class="user-id-text">${username}(${userIdDisplay})</span>
                                    <div style="display: flex; gap: 5px;">
                                        <button class="sa-user-detail-btn" onclick="event.stopPropagation(); saViewUserDetail('${escapedUserId}', '${username}')">详情</button>
                                    </div>
                                </div>
                                <div class="user-button-stats">
                                    <div class="user-stat-item">
                                        <span class="user-stat-label">rate:</span>
                                        <span class="user-stat-value">${sendRate}</span>
                                    </div>
                                    <div class="user-stat-item">
                                        <span class="user-stat-label">balance:</span>
                                        <span class="user-stat-value">$${balance}</span>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                    `;
    }).join('');
}
// 查看用户详情
// 查看用户详情
function saViewUserDetail(userId, username) {
    if (!userId) return;

    const user = saAllUsersData.find(u => String(u.user_id) === String(userId));

    if (user) {
        const message = `
用户详情:
用户名: ${username}
用户ID: ${userId}
余额: ${(user.credits || 0).toFixed(2)}
发送速率: ${user.send_rate || '0.00'}
服务器数量: ${user.server_count || 0}
创建时间: ${user.created_at ? new Date(user.created_at).toLocaleString() : '未知'}
        `.trim();

        appendSuperAdminLog(message, 'info');
    } else {
        appendSuperAdminLog(`未找到用户 ${userId} 的详情`, 'warning');
    }
}
// 查看管理员详情
// 查看管理员详情
function saOpenAdminDetailModal(html) {
    const modal = document.getElementById('saAdminDetailModal');
    const content = document.getElementById('saAdminDetailContent');
    if (!modal || !content) return;
    content.innerHTML = html || '<div style="color:#ddd;">暂无详情</div>';
    modal.style.display = 'flex';
}

function saCloseAdminDetailModal() {
    const modal = document.getElementById('saAdminDetailModal');
    if (!modal) return;
    modal.style.display = 'none';
}

function saViewAdminDetail(adminId, username) {
    if (!adminId) return;

    const admin = saAllAdminsData.find(a => String(a.admin_id || a.id) === String(adminId));

    if (admin) {
        const serverCount = Array.isArray(admin.selected_servers) ? admin.selected_servers.length : 0;
        const html = `
            <div style="display:flex; flex-direction:column; gap:8px; color:#fff; font-size:14px;">
                <div>管理员: <b>${username || adminId}</b></div>
                <div>管理员ID: ${adminId}</div>
                <div>管理服务器数: ${serverCount}</div>
                <div>用户数: ${admin.user_count || 0}</div>
                <div>业绩: ${Number(admin.performance || 0).toFixed(2)}</div>
                <div>创建时间: ${admin.created ? new Date(admin.created).toLocaleString() : '-'}</div>
            </div>
        `;
        saOpenAdminDetailModal(html);
    } else {
        appendSuperAdminLog(`未找到管理员 ${adminId} 的详情`, 'warning');
    }
}
// 筛选管理员列表
function saFilterAdminList(keyword) {
    if (!keyword) {
        saRenderAdminList(saAllAdminsData);
        return;
    }
    const lower = keyword.toLowerCase();
    const filtered = saAllAdminsData.filter(a =>
        (a.id && a.id.toLowerCase().includes(lower)) ||
        (a.username && a.username.toLowerCase().includes(lower))
    );
    saRenderAdminList(filtered);
}
// 导出用户数据
// 导出用户数据
function saExportUserData() {
    if (!saAllUsersData || saAllUsersData.length === 0) {
        appendSuperAdminLog('没有可导出的用户数据', 'warning');
        return;
    }

    const csv = [
        ['序号', '用户名', '用户ID', '余额', '发送量', '成功率', '注册时间'].join(','),
        ...saAllUsersData.map((u, index) => {
            const username = u.username || 'No Name';
            const uid = u.user_id;
            const balance = (u.credits || 0).toFixed(2);
            const sent = u.last_sent || 0;
            const success = u.total_success || 0;
            const fail = u.total_fail || 0;
            const total = success + fail;
            const successRate = total > 0 ? ((success / total) * 100).toFixed(1) : '0.0';
            const created = u.created_at ? new Date(u.created_at).toLocaleDateString('zh-CN') : '-';
            return [index + 1, username, uid, balance, sent, successRate + '%', created].join(',');
        })
    ].join('\n');

    const blob = new Blob(['\ufeff' + csv], { type: 'text/csv;charset=utf-8;' });
    const link = document.createElement('a');
    link.href = URL.createObjectURL(blob);
    link.download = `用户数据_${new Date().toISOString().split('T')[0]}.csv`;
    link.click();
    appendSuperAdminLog('用户数据导出成功', 'success');
}
// 导出管理员数据
function saExportAdminData() {
    if (!saAllAdminsData || saAllAdminsData.length === 0) {
        appendSuperAdminLog('没有可导出的管理员数据', 'warning');
        return;
    }

    const csv = [
        ['序号', '管理员ID', '用户名', '管理用户数', '总业绩', '在线状态', '创建时间'].join(','),
        ...saAllAdminsData.map((a, index) => {
            const adminId = a.id || a.admin_id || 'Unknown';
            const username = a.username || adminId;
            const userCount = a.user_count || 0;
            const performance = a.performance || 0;
            const online = a.online === true ? '在线' : '离线';
            const created = a.created_at ? new Date(a.created_at).toLocaleDateString('zh-CN') : '-';
            return [index + 1, adminId, username, userCount, performance, online, created].join(',');
        })
    ].join('\n');

    const blob = new Blob(['\ufeff' + csv], { type: 'text/csv;charset=utf-8;' });
    const link = document.createElement('a');
    link.href = URL.createObjectURL(blob);
    link.download = `管理员数据_${new Date().toISOString().split('T')[0]}.csv`;
    link.click();
    appendSuperAdminLog('管理员数据导出成功', 'success');
}

//#endregion
//#region ==================== 日志控制设置 ====================
// 设置日志控制
function setupSuperAdminLogControls() {

    // 日志缓冲区，批量发送
    const logBuffer = [];
    let sendTimer = null;
    const BATCH_SIZE = 10;
    const BATCH_INTERVAL = 5000; // 5秒批量发送一次

    // 过滤不需要保存的日志
    function shouldSaveLog(message) {
        // 过滤HTTP请求日志
        if (message.includes('POST /api/admin/logs/save') ||
            message.includes('GET /api/admin/logs') ||
            message.includes('HTTP/1.1')) {
            return false;
        }
        // 过滤心跳相关日志
        if (message.toLowerCase().includes('ping') ||
            message.toLowerCase().includes('pong') ||
            message.toLowerCase().includes('心跳')) {
            return false;
        }
        return true;
    }

    // 批量发送日志
    function sendLogBatch() {
        if (logBuffer.length === 0) return;

        const logsToSend = logBuffer.splice(0, BATCH_SIZE);

        try {
            // 🔥 安全检查：确保 API_BASE_URL 已定义
            const apiUrl = (typeof API_BASE_URL !== 'undefined' && API_BASE_URL) ? API_BASE_URL : '/api';
            // 连续失败退避/熔断，避免 524 风暴把后端压死
            window.__logSendFailCount = window.__logSendFailCount || 0;
            window.__logSendBackoffMs = window.__logSendBackoffMs || BATCH_INTERVAL;
            if (window.__logSendDisabled) return;
            fetch(`${apiUrl}/admin/logs/save`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    type: 'html',
                    logs: logsToSend
                })
            })
                .then(res => {
                    if (!res.ok) throw new Error(String(res.status));
                    window.__logSendFailCount = 0;
                    window.__logSendBackoffMs = BATCH_INTERVAL;
                })
                .catch((e) => {
                    window.__logSendFailCount = (window.__logSendFailCount || 0) + 1;
                    const msg = String(e && (e.message || e) || '');
                    if (msg.includes('524') && window.__logSendFailCount >= 3) {
                        window.__logSendDisabled = true;
                        // 只打印一次，避免刷屏
                        originalWarn('[前端日志] /api/admin/logs/save 连续 524，已自动停用上报（避免压垮后端）');
                        return;
                    }
                    window.__logSendBackoffMs = Math.min(60000, Math.floor((window.__logSendBackoffMs || BATCH_INTERVAL) * 1.8));
                });
        } catch (e) { }

        // 如果还有日志，继续发送
        if (logBuffer.length > 0) {
            const delay = (window.__logSendBackoffMs || BATCH_INTERVAL);
            sendTimer = setTimeout(sendLogBatch, delay);
        } else {
            sendTimer = null;
        }
    }

    // 添加日志到缓冲区
    function addToBuffer(level, message) {
        if (!shouldSaveLog(message)) return;
        // 如果已经熔断日志上报，就别继续堆积内存了（否则长时间会把页面拖死）
        if (window.__logSendDisabled) return;

        logBuffer.push({
            level: level.toUpperCase(),
            message: message,
            detail: {},
            ts: new Date().toISOString()
        });

        // 防止缓冲区无限增长导致页面卡死
        const MAX_BUFFER = 200;
        if (logBuffer.length > MAX_BUFFER) {
            logBuffer.splice(0, logBuffer.length - MAX_BUFFER);
        }

        // 如果缓冲区满了，立即发送
        if (logBuffer.length >= BATCH_SIZE) {
            if (sendTimer) {
                clearTimeout(sendTimer);
                sendTimer = null;
            }
            sendLogBatch();
        } else if (!sendTimer) {
            // 否则设置定时器批量发送
            sendTimer = setTimeout(sendLogBatch, BATCH_INTERVAL);
        }
    }
}
//#endregion
//#region ==================== Worker远程控制面板功能 ====================

// Worker相关变量
let workerCommandHistory = [];
let workerAccounts = []; // 存储当前Worker的账号列表
let selectedWorkerAccount = null;
let wcCaptchaDialogTimer = null;
let wcCaptchaDialogActive = false;
let wcCaptchaLastAccount = '';

// 渲染雷达机器人服务器列表
// 渲染雷达机器人
function renderRadarBots(servers) {
    const grid = document.getElementById('superAdminServersList');
    if (!grid) return;

    if (!servers || servers.length === 0) {
        grid.innerHTML = `
        <div class="empty-state" style="grid-column: 1 / -1;">
            <span class="icon">📡</span>
            <span class="text">No online servers</span>
        </div>
    `;
        return;
    }

    grid.innerHTML = servers.map(server => {
        const serverId = server.server_id || server.server_name || 'Unknown';
        const meta = server.meta || {};
        const status = determineWorkerStatus(server);

        return `
        <div class="radar-bot status-${status.type}" data-server-id="${serverId}" onclick="openWorkerRemotePanel('${serverId}')">
            <div class="radar-bot-icon"></div>
            <div class="radar-bot-status ${status.type}" title="${status.text}"></div>
            <div class="radar-bot-name">${meta.phone || serverId.slice(0, 8)}</div>
            <div class="radar-bot-id">${serverId.slice(0, 12)}...</div>
            <div class="radar-bot-status-icon">${status.icon}</div>
        </div>
    `;
    }).join('');
}

// 判断Worker状态（基于最近两次shard处理结果）
// 状态逻辑：
// - 最后一次成功 -> Good (绿色)
// - 最后一次失败但前一次成功 -> Warning (黄色，需要检测)
// - 连续两次失败 -> Error (红色，故障状态)
// 判断Worker状态
function determineWorkerStatus(server) {
    const meta = server.meta || {};
    const shardHistory = meta.shard_history || [];
    const now = Date.now();
    const lastSeen = server.last_seen ? new Date(server.last_seen).getTime() : 0;
    const timeSinceLastSeen = now - lastSeen;

    if (!isSuperAdminServerOnline(server, now)) {
        return { type: 'error', text: 'Offline', icon: '🔴' };
    }

    if (timeSinceLastSeen > 120000) {
        return { type: 'error', text: 'Offline', icon: '🔴' };
    }

    if (shardHistory.length === 0) {
        if (timeSinceLastSeen < 30000) {
            return { type: 'good', text: 'Online', icon: '🟢' };
        }
        return { type: 'warning', text: 'Idle', icon: '🟡' };
    }

    const lastResult = shardHistory[shardHistory.length - 1];
    const prevResult = shardHistory.length > 1 ? shardHistory[shardHistory.length - 2] : null;

    const lastSuccess = lastResult && (lastResult.success > 0) && (lastResult.fail === 0 || lastResult.fail === undefined);

    if (lastSuccess) {
        return { type: 'good', text: 'Good', icon: '🟢' };
    }

    if (prevResult) {
        const prevSuccess = prevResult.success > 0 && (prevResult.fail === 0 || prevResult.fail === undefined);

        if (prevSuccess) {
            return { type: 'warning', text: 'Check Required', icon: '🟡' };
        } else {
            return { type: 'error', text: 'Error', icon: '🔴' };
        }
    } else {
        return { type: 'warning', text: 'Check Required', icon: '🟡' };
    }
}

// 打开Worker控制面板 - 替换整个右侧内容
// 打开Worker控制面板
async function openWorkerRemotePanel(serverId) {
    currentRemoteWorkerId = null;

    // 查找服务器数据
    const server = superAdminServers.find(s => (s.server_id || s.server_name) === serverId);
    if (!server) {
        saShowToast('Server not found', 'error');
        return;
    }

    currentRemoteWorkerId = serverId;
    currentRemoteWorkerData = server;

    // 更新面板信息
    await updateWorkerControlPanelInfo(server);

    // 隐藏服务器列表，显示Worker控制面板
    document.getElementById('superAdminServersSection').style.display = 'none';
    document.getElementById('superAdminDetailSection').style.display = 'flex';

    // 清空控制台
    clearWorkerConsole();
    appendWorkerConsole(`[${currentRemoteWorkerId}] Connected.`, 'success');

    loadWcIdLibrary();

    // 系统状态：不做任何轮询，仅在打开面板时静默触发一次并拉取一次
    try {
        requestWcSystemStatusOnceSilent();
    } catch (e) {
        // 完全静默
    }
}

async function requestWcSystemStatusOnceSilent() {
    if (!currentRemoteWorkerId) return;

    // 先发一个命令让 worker 采集（如果 worker/WS 不在线，这一步也会失败，但不影响页面）
    try {
        await fetch(`${API_BASE_URL}/super-admin/worker/${encodeURIComponent(currentRemoteWorkerId)}/control`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ action: 'system_status', params: {} })
        });
    } catch (e) {
        // 静默
    }

    // 稍等让后端缓存落地，然后拉一次状态
    try {
        await new Promise(resolve => setTimeout(resolve, 1200));
        await loadWcSysStatusContent();
    } catch (e) {
        // 静默
    }
}

// 关闭Worker控制面板 - 返回服务器列表
// 关闭Worker控制面板
function closeWorkerControlPanel() {
    stopWcSysStatusRetry();

    const detailSection = document.getElementById('superAdminDetailSection');
    const serversSection = document.getElementById('superAdminServersSection');

    if (detailSection) detailSection.style.display = 'none';
    if (serversSection) serversSection.style.display = 'block';

    currentRemoteWorkerId = null;
    currentRemoteWorkerData = null;
    selectedWorkerAccount = null;

    if (typeof loadSuperAdminServers === 'function') {
        loadSuperAdminServers();
    }
}

// 保持旧函数名兼容
// 关闭Worker远程面板
function closeWorkerRemotePanel() {
    closeWorkerControlPanel();
}

// 更新Worker控制面板信息
// 更新Worker面板信息
async function updateWorkerControlPanelInfo(server) {
    const meta = server.meta || {};
    const status = determineWorkerStatus(server);
    const now = Date.now();
    const lastSeen = server.last_seen ? new Date(server.last_seen).getTime() : 0;
    const uptimeSeconds = lastSeen ? Math.floor((now - lastSeen) / 1000) : 0;
    const uptimeStr = uptimeSeconds > 3600
        ? `${Math.floor(uptimeSeconds / 3600)}h ${Math.floor((uptimeSeconds % 3600) / 60)}m`
        : uptimeSeconds > 60
            ? `${Math.floor(uptimeSeconds / 60)}m`
            : `${uptimeSeconds}s`;

    // 从API获取Worker本地配置数据
    let localConfig = {};
    try {
        const configRes = await fetch(`${API_BASE_URL}/super-admin/worker/${server.server_id}/config`);
        const configData = await configRes.json();
        if (configData.success && configData.data) {
            localConfig = configData.data;
        }
    } catch (e) {
        console.log('获取本地配置失败', e);
    }

    const workerIdDisplay = document.getElementById('wcWorkerIdDisplay');
    if (workerIdDisplay) {
        workerIdDisplay.textContent = server.server_id || server.server_name || '-';
    }

    const statusBadge = document.getElementById('workerControlStatus');
    if (statusBadge) {
        const label = statusBadge.querySelector('.status-text');

        if (status.type === 'good') {
            statusBadge.classList.remove('error');
            if (label) label.textContent = 'Running';
        } else if (status.type === 'error') {
            statusBadge.classList.add('error');
            if (label) label.textContent = 'Error';
        } else {
            statusBadge.classList.remove('error');
            if (label) label.textContent = 'Warning';
        }
    }

    const setVal = (id, val) => {
        const el = document.getElementById(id);
        if (el) el.textContent = val || '-';
    };

    // 优先使用Worker本地配置，其次使用数据库数据
    const phone = localConfig.server_phone || meta.phone || server.phone || '';
    const email = localConfig.email || meta.email || server.email || '';
    const apiUrl = localConfig.api_url || server.server_url || server.url || '';
    const port = localConfig.port || server.port || '';

    setVal('wcPhone', phone);
    setVal('wcEmail', email);
    setVal('wcApiUrl', apiUrl || '-');
    setVal('wcPort', port);

    setVal('wcUptime', meta.uptime || uptimeStr);

    // 从stats中获取数据（优先使用Worker本地配置）
    const stats = localConfig.stats || meta.stats || {};
    const shardsProcessed = stats.shards || meta.shards_processed || server.shards_processed || server.shard_count || 0;
    const messagesSent = stats.sent || meta.messages_sent || server.messages_sent || server.message_count || 0;
    const successCount = stats.success || 0;
    const failedCount = stats.failed || 0;

    // 计算成功率
    const total = successCount + failedCount;
    const successRate = total > 0 ? Math.round((successCount / total) * 100) : 0;

    setVal('wcShards', String(shardsProcessed));
    setVal('wcMessages', String(messagesSent));
    setVal('wcSuccessRate', successRate + '%');

    updateCurrentAccountDisplay(meta);
}

let wcSysStatusRetryTimer = null;
let wcSysStatusRetryCount = 0;

function startWcSysStatusRetry() {
    // 禁用自动轮询/重试：保留空函数仅用于兼容旧调用点
    return;
}

async function loadWcSysStatusContent() {
    const contentEl = document.getElementById('wcSysStatusContent');
    if (!contentEl) return;

    if (!currentRemoteWorkerId) {
        contentEl.innerHTML = '<div class="wc-sys-loading">请先选择服务器</div>';
        return;
    }

    try {
        const statusResponse = await fetch(`${API_BASE_URL}/super-admin/worker/${encodeURIComponent(currentRemoteWorkerId)}/system-status`, {
            method: 'GET'
        });
        const statusData = await statusResponse.json();

        if (statusData.success && statusData.data) {
            const s = statusData.data;
            contentEl.innerHTML = renderSystemStatusHtml(s);
            return;
        }
    } catch (e) {
        // 完全静默
    }

    contentEl.innerHTML = '<div class="wc-sys-error">未知状态</div>';
}

function renderSystemStatusHtml(s) {
    return `
        <div class="status-row">
            <span class="s-label">CPU</span>
            <span class="s-value ${s.cpu_percent > 80 ? 'danger' : 'good'}">${s.cpu_percent || 0}%</span>
        </div>
        <div class="status-row">
            <span class="s-label">内存</span>
            <span class="s-value ${s.memory_percent > 80 ? 'danger' : 'good'}">${s.memory_percent || 0}%</span>
        </div>
        <div class="status-row">
            <span class="s-label">内存用量</span>
            <span class="s-value">${s.memory_used || 0} / ${s.memory_total || 0} GB</span>
        </div>
        <div class="status-row">
            <span class="s-label">磁盘</span>
            <span class="s-value ${s.disk_percent > 80 ? 'danger' : 'good'}">${s.disk_percent || 0}%</span>
        </div>
        <div class="status-row">
            <span class="s-label">可用空间</span>
            <span class="s-value">${s.disk_free || 0} GB</span>
        </div>
        <div class="status-row">
            <span class="s-label">运行时长</span>
            <span class="s-value warning">${s.uptime_hours || 0} 小时</span>
        </div>
        <div class="status-row">
            <span class="s-label">平台</span>
            <span class="s-value">${s.platform || 'Unknown'}</span>
        </div>
        <div class="status-row">
            <span class="s-label">Python</span>
            <span class="s-value">${s.python_version || 'Unknown'}</span>
        </div>
    `;
}

function stopWcSysStatusRetry() {
    if (wcSysStatusRetryTimer) {
        clearTimeout(wcSysStatusRetryTimer);
        wcSysStatusRetryTimer = null;
    }
    wcSysStatusRetryCount = 0;
}

// 更新当前账号显示
// 更新当前账号显示
function updateCurrentAccountDisplay(meta) {
    const accountIcon = document.getElementById('wcAccountIcon');
    const accountValue = document.getElementById('wcCurrentAccount');

    if (meta.logged_in && meta.current_account) {
        if (accountIcon) accountIcon.textContent = '🔒';
        if (accountValue) accountValue.textContent = meta.current_account;
    } else {
        if (accountIcon) accountIcon.textContent = '🔓';
        if (accountValue) accountValue.textContent = '未登录';
    }
}

// 点击编辑功能
// 使元素可编辑
function makeEditable(element, fieldName) {
    if (element.classList.contains('editing')) return;

    const originalValue = element.textContent;
    if (originalValue === '-') {
        element.textContent = '';
    }

    element.classList.add('editing');
    element.contentEditable = true;
    element.focus();

    // 保存原始值用于取消
    element.dataset.originalValue = originalValue;
    element.dataset.fieldName = fieldName;

    // 选中文本
    const range = document.createRange();
    range.selectNodeContents(element);
    const sel = window.getSelection();
    sel.removeAllRanges();
    sel.addRange(range);

    // 添加保存提示
    element.title = '按Enter保存，按Esc取消';
}

// 处理编辑完成
document.addEventListener('keydown', async function (e) {
    if (e.target.classList.contains('val-editable') && e.target.classList.contains('editing')) {
        if (e.key === 'Enter') {
            e.preventDefault();
            await saveEditableValue(e.target);
        } else if (e.key === 'Escape') {
            e.preventDefault();
            cancelEditable(e.target);
        }
    }
});

// 处理点击外部取消编辑
document.addEventListener('click', async function (e) {
    if (e.target.classList.contains('val-editable') && e.target.classList.contains('editing')) {
        return;
    }

    const editingElements = document.querySelectorAll('.val-editable.editing');
    for (const el of editingElements) {
        cancelEditable(el);
    }
});

// 保存编辑值
// 保存编辑值
async function saveEditableValue(element) {
    if (!element.classList.contains('editing')) return;

    const fieldName = element.dataset.fieldName;
    const newValue = element.textContent.trim();

    if (!currentRemoteWorkerId) {
        saShowToast('请先选择服务器', 'error');
        cancelEditable(element);
        return;
    }

    if (newValue === element.dataset.originalValue || newValue === '') {
        cancelEditable(element);
        return;
    }

    appendWorkerConsole(`正在更新 ${fieldName}: ${newValue}...`, 'command');

    try {
        const response = await fetch(`${API_BASE_URL}/super-admin/worker/${encodeURIComponent(currentRemoteWorkerId)}/meta`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                field: fieldName,
                value: newValue
            })
        });

        const data = await response.json();

        if (data.success) {
            element.classList.remove('editing');
            element.contentEditable = false;
            element.title = '';
            appendWorkerConsole(`${fieldName} 更新成功`, 'success');
            saShowToast(`${fieldName} 已更新`, 'success');
        } else {
            appendWorkerConsole(`更新失败: ${data.message}`, 'error');
            saShowToast(data.message || '更新失败', 'error');
            cancelEditable(element);
        }
    } catch (e) {
        appendWorkerConsole(`更新失败: ${e.message}`, 'error');
        saShowToast('网络错误', 'error');
        cancelEditable(element);
    }
}

// 取消编辑
// 取消编辑
function cancelEditable(element) {
    element.classList.remove('editing');
    element.contentEditable = false;
    element.textContent = element.dataset.originalValue || '-';
    element.title = '';
}

// 切换编辑字段
// 切换编辑字段
function toggleEditField(fieldName) {
    const displayEl = document.getElementById('wc' + fieldName + 'Display');
    const inputEl = document.getElementById('wc' + fieldName + 'Input');
    const saveBtn = document.getElementById('wc' + fieldName + 'SaveBtn');

    if (!displayEl || !inputEl || !saveBtn) return;

    const isEditing = inputEl.style.display !== 'none';

    if (isEditing) {
        inputEl.style.display = 'none';
        displayEl.style.display = 'inline';
        saveBtn.style.display = 'none';
    } else {
        inputEl.style.display = 'inline';
        displayEl.style.display = 'none';
        saveBtn.style.display = 'inline';
        inputEl.value = displayEl.textContent === '-' ? '' : displayEl.textContent;
        inputEl.focus();
    }
}

// 保存字段
// 保存字段值
async function saveField(fieldName) {
    const inputEl = document.getElementById('wc' + fieldName + 'Input');
    const displayEl = document.getElementById('wc' + fieldName + 'Display');
    const saveBtn = document.getElementById('wc' + fieldName + 'SaveBtn');

    if (!inputEl || !displayEl) return;

    const value = inputEl.value.trim();

    if (!currentRemoteWorkerId) {
        saShowToast('请先选择服务器', 'error');
        return;
    }

    appendWorkerConsole(`正在更新 ${fieldName}: ${value}...`, 'command');

    try {
        const response = await fetch(`${API_BASE_URL}/super-admin/worker/${encodeURIComponent(currentRemoteWorkerId)}/meta`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                field: fieldName.toLowerCase(),
                value: value
            })
        });

        const data = await response.json();

        if (data.success) {
            displayEl.textContent = value || '-';
            inputEl.style.display = 'none';
            displayEl.style.display = 'inline';
            if (saveBtn) saveBtn.style.display = 'none';
            appendWorkerConsole(`${fieldName} 更新成功`, 'success');
            saShowToast(`${fieldName} 已更新`, 'success');
        } else {
            appendWorkerConsole(`更新失败: ${data.message}`, 'error');
            saShowToast(data.message || '更新失败', 'error');
        }
    } catch (e) {
        appendWorkerConsole(`更新失败: ${e.message}`, 'error');
        saShowToast('网络错误', 'error');
    }
}

// 切换密码显示
// 切换密码显示
function toggleWcPassword() {
    const input = document.getElementById('wcLoginPassword');
    if (input) {
        input.type = input.type === 'password' ? 'text' : 'password';
    }
}

// Worker登录
// Worker登录iMessage
async function wcLogin() {
    const appleId = document.getElementById('wcLoginId').value.trim();
    const password = document.getElementById('wcLoginPassword').value;

    if (!appleId || !password) {
        saShowToast('请输入Apple ID和密码', 'warning');
        return;
    }

    if (!currentRemoteWorkerId) {
        saShowToast('请先选择服务器', 'error');
        return;
    }

    wcCaptchaLastAccount = appleId;
    openWcCaptchaDialog(appleId);

    appendWorkerConsole(`正在登录: ${appleId}...`, 'command');

    try {
        const response = await fetch(`${API_BASE_URL}/super-admin/worker/${encodeURIComponent(currentRemoteWorkerId)}/control`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                action: 'login',
                // 兼容 worker 侧可能使用 account/password 或 apple_id/password
                params: { apple_id: appleId, account: appleId, password: password }
            })
        });

        const data = await response.json();

        if (data.success) {
            appendWorkerConsole(`登录成功: ${appleId}`, 'success');
            saShowToast('登录成功', 'success');
            setWcCaptchaStatus('登录指令执行完成，若有验证码请填写');

            const accountIcon = document.getElementById('wcAccountIcon');
            const accountValue = document.getElementById('wcCurrentAccount');
            if (accountIcon) accountIcon.textContent = '🔒';
            if (accountValue) accountValue.textContent = appleId;

            document.getElementById('wcLoginId').value = '';
            document.getElementById('wcLoginPassword').value = '';
        } else {
            appendWorkerConsole(`登录失败: ${data.message}`, 'error');
            saShowToast(data.message || '登录失败', 'error');
            setWcCaptchaStatus('登录失败: ' + (data.message || '未知错误'));
        }
    } catch (e) {
        appendWorkerConsole(`登录失败: ${e.message}`, 'error');
        saShowToast('网络错误', 'error');
        setWcCaptchaStatus('登录异常: ' + e.message);
    }
}

// Worker退出登录
// Worker退出登录
async function wcLogout() {
    if (!currentRemoteWorkerId) {
        saShowToast('请先选择服务器', 'error');
        return;
    }

    if (!confirm('确定要退出当前账号吗？')) return;

    appendWorkerConsole('正在退出登录...', 'command');

    try {
        const response = await fetch(`${API_BASE_URL}/super-admin/worker/${encodeURIComponent(currentRemoteWorkerId)}/control`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                action: 'logout',
                params: {}
            })
        });

        const data = await response.json();

        if (data.success) {
            appendWorkerConsole('已退出登录', 'success');
            saShowToast('已退出登录', 'success');

            const accountIcon = document.getElementById('wcAccountIcon');
            const accountValue = document.getElementById('wcCurrentAccount');
            if (accountIcon) accountIcon.textContent = '🔓';
            if (accountValue) accountValue.textContent = '未登录';
        } else {
            appendWorkerConsole(`退出失败: ${data.message}`, 'error');
            saShowToast(data.message || '退出失败', 'error');
        }
    } catch (e) {
        appendWorkerConsole(`退出失败: ${e.message}`, 'error');
        saShowToast('网络错误', 'error');
    }
}

// 加载Worker ID库
// 加载Worker账号库
let wcIdLibraryData = [];
let wcIdLibraryVisible = false;

async function loadWcIdLibrary() {
    try {
        const response = await fetch(`${API_BASE_URL}/id-library`, {
            method: 'GET',
            headers: { 'Content-Type': 'application/json' }
        });

        const data = await response.json();

        if (data.success && data.accounts) {
            wcIdLibraryData = data.accounts;
            renderWcIdList(data.accounts);
        } else {
            wcIdLibraryData = [];
            renderWcIdList([]);
        }
    } catch (e) {
        wcIdLibraryData = [];
        renderWcIdList([]);
    }
}

function toggleWcIdList() {
    const listEl = document.getElementById('wcIdList');
    if (!listEl) return;

    wcIdLibraryVisible = !wcIdLibraryVisible;
    listEl.style.display = wcIdLibraryVisible ? 'block' : 'none';
}

// 渲染ID库列表
function renderWcIdList(accounts) {
    const listEl = document.getElementById('wcIdList');
    if (!listEl) return;

    listEl.style.display = wcIdLibraryVisible ? 'block' : 'none';

    if (!accounts || accounts.length === 0) {
        listEl.innerHTML = '<div class="wc-id-empty">暂无账号</div>';
        return;
    }

    listEl.innerHTML = accounts.map((acc, idx) => {
        const status = acc.status || 'normal';
        const statusClass = status === 'fault' ? 'error' : '';
        const statusText = status === 'fault' ? '故障' : '正常';

        return `
        <div class="wc-id-item" data-idx="${idx}">
            <span class="id-name">${acc.appleId || acc.account}</span>
            <span class="id-status ${statusClass}">${statusText}</span>
        </div>
    `;
    }).join('');

    listEl.querySelectorAll('.wc-id-item').forEach(item => {
        item.addEventListener('click', () => {
            const idx = parseInt(item.getAttribute('data-idx') || '-1', 10);
            const acc = (Number.isFinite(idx) && idx >= 0) ? wcIdLibraryData[idx] : null;
            if (!acc) return;
            fillWcLogin(acc.appleId || acc.account || '', acc.password || '', false);
        });

        item.addEventListener('dblclick', () => {
            const idx = parseInt(item.getAttribute('data-idx') || '-1', 10);
            const acc = (Number.isFinite(idx) && idx >= 0) ? wcIdLibraryData[idx] : null;
            if (!acc) return;
            fillWcLogin(acc.appleId || acc.account || '', acc.password || '', true);
        });
    });
}


// 填充登录信息
function fillWcLogin(account, password, fillPassword = true) {
    const idInput = document.getElementById('wcLoginId');
    const pwInput = document.getElementById('wcLoginPassword');

    if (idInput) idInput.value = account || '';
    if (fillPassword && pwInput) pwInput.value = password || '';
    if (account) {
        selectedWorkerAccount = account;
    }

    saShowToast('已填充账号信息', 'success');
}

function openWcScreenshotModal() {
    const imgEl = document.getElementById('wcScreenshotImg');
    const modal = document.getElementById('wcScreenshotModal');
    const modalImg = document.getElementById('wcScreenshotModalImg');
    if (!imgEl || !modal || !modalImg) return;
    if (!imgEl.src) return;
    modalImg.src = imgEl.src;
    modal.style.display = 'flex';
}

function closeWcScreenshotModal() {
    const modal = document.getElementById('wcScreenshotModal');
    const modalImg = document.getElementById('wcScreenshotModalImg');
    if (!modal) return;
    modal.style.display = 'none';
    if (modalImg) modalImg.src = '';
}

// 远程截图
// 远程截取屏幕
async function takeWorkerScreenshot() {
    if (!currentRemoteWorkerId) {
        saShowToast('请先选择服务器', 'error');
        return;
    }

    appendWorkerConsole('正在截图...', 'command');

    try {
        const image = await requestWorkerScreenshotImage();
        const imgEl = document.getElementById('wcScreenshotImg');
        const areaEl = document.getElementById('wcScreenshotArea');

        if (imgEl && areaEl) {
            imgEl.src = 'data:image/png;base64,' + image;
            areaEl.style.display = 'block';
            appendWorkerConsole('截图已加载', 'success');
        }
    } catch (e) {
        const areaEl = document.getElementById('wcScreenshotArea');
        if (areaEl) areaEl.style.display = 'none';
        appendWorkerConsole('截图失败: ' + e.message, 'error');
    }
}

async function fetchWorkerScreenshotFromServer() {
    if (!currentRemoteWorkerId) {
        throw new Error('请先选择服务器');
    }

    const imgResponse = await fetch(`${API_BASE_URL}/super-admin/worker/${encodeURIComponent(currentRemoteWorkerId)}/screenshot`);
    const imgData = await imgResponse.json();

    if (!(imgData.success && imgData.data && imgData.data.image)) {
        throw new Error(imgData.message || '未获取到最新截图');
    }

    return imgData.data.image;
}

function updateScreenshotArea(imageBase64, logMessage = '截图已更新') {
    const imgEl = document.getElementById('wcScreenshotImg');
    const areaEl = document.getElementById('wcScreenshotArea');
    if (imgEl && areaEl) {
        imgEl.src = 'data:image/png;base64,' + imageBase64;
        areaEl.style.display = 'block';
        appendWorkerConsole(logMessage, 'success');
    }
}

async function viewAccountScreenshot() {
    if (!currentRemoteWorkerId) {
        saShowToast('请先选择服务器', 'error');
        return;
    }

    appendWorkerConsole('正在打开账户面板截图...', 'command');
    try {
        await sendWorkerCommand('open_account_panel');
        const image = await fetchWorkerScreenshotFromServer();
        updateScreenshotArea(image, '账户截图已加载');
    } catch (e) {
        appendWorkerConsole('获取账户截图失败: ' + e.message, 'error');
        saShowToast('获取账户截图失败: ' + e.message, 'error');
    }
}

async function requestWorkerScreenshotImage() {
    if (!currentRemoteWorkerId) {
        throw new Error('请先选择服务器');
    }

    const response = await fetch(`${API_BASE_URL}/super-admin/worker/${encodeURIComponent(currentRemoteWorkerId)}/control`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action: 'screenshot', params: {} })
    });

    const data = await response.json();
    if (!data.success) {
        throw new Error(data.message || '截图命令失败');
    }

    await new Promise(resolve => setTimeout(resolve, 2000));

    const imgResponse = await fetch(`${API_BASE_URL}/super-admin/worker/${encodeURIComponent(currentRemoteWorkerId)}/screenshot`);
    const imgData = await imgResponse.json();

    if (!(imgData.success && imgData.data && imgData.data.image)) {
        throw new Error(imgData.message || '未从Worker获得截图');
    }

    return imgData.data.image;
}

function setWcCaptchaStatus(text) {
    const statusEl = document.getElementById('wcCaptchaStatus');
    if (statusEl) statusEl.textContent = text;
}

function hideWcCaptchaInputGroup() {
    const group = document.getElementById('wcCaptchaInputGroup');
    if (group) group.style.display = 'none';
}

function showWcCaptchaInputGroup() {
    const group = document.getElementById('wcCaptchaInputGroup');
    if (group) {
        group.style.display = 'flex';
        const input = document.getElementById('wcCaptchaInput');
        if (input) {
            input.focus();
            input.select();
        }
    }
}

function openWcCaptchaDialog(account) {
    const dialog = document.getElementById('wcCaptchaDialog');
    if (!dialog) return;
    wcCaptchaLastAccount = account || '';
    dialog.style.display = 'flex';
    wcCaptchaDialogActive = true;
    const screenshot = document.getElementById('wcCaptchaScreenshot');
    if (screenshot) screenshot.src = '';
    const input = document.getElementById('wcCaptchaInput');
    if (input) input.value = '';
    hideWcCaptchaInputGroup();
    setWcCaptchaStatus('正在发起登录...');
    refreshWcCaptchaScreenshot();
}

function closeWcCaptchaDialog() {
    wcCaptchaDialogActive = false;
    if (wcCaptchaDialogTimer) {
        clearTimeout(wcCaptchaDialogTimer);
        wcCaptchaDialogTimer = null;
    }
    const dialog = document.getElementById('wcCaptchaDialog');
    if (dialog) dialog.style.display = 'none';
    hideWcCaptchaInputGroup();
}

async function refreshWcCaptchaScreenshot() {
    if (!wcCaptchaDialogActive) return;
    try {
        const image = await requestWorkerScreenshotImage();
        const imgEl = document.getElementById('wcCaptchaScreenshot');
        if (imgEl) imgEl.src = 'data:image/png;base64,' + image;
        setWcCaptchaStatus('截图已更新，若看到验证码请输入');
        showWcCaptchaInputGroup();
    } catch (e) {
        setWcCaptchaStatus('截图失败: ' + e.message);
    } finally {
        if (wcCaptchaDialogActive) {
            wcCaptchaDialogTimer = setTimeout(refreshWcCaptchaScreenshot, 2800);
        }
    }
}

async function submitWcCaptchaCode() {
    const codeInput = document.getElementById('wcCaptchaInput');
    const code = (codeInput ? codeInput.value : '').trim();
    if (!code) {
        saShowToast('请输入验证码', 'warning');
        return;
    }
    if (!currentRemoteWorkerId) {
        saShowToast('请先选择服务器', 'error');
        return;
    }

    setWcCaptchaStatus('正在提交验证码...');
    appendWorkerConsole('提交验证码...', 'command');

    try {
        const response = await fetch(`${API_BASE_URL}/super-admin/worker/${encodeURIComponent(currentRemoteWorkerId)}/control`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ action: 'submit_2fa_code', params: { code } })
        });

        const data = await response.json();

        if (data.success) {
            setWcCaptchaStatus('验证码提交成功');
            const confirmed = await customConfirm(`是否更新账号状态为 ${wcCaptchaLastAccount || '当前账号'}?`);
            if (confirmed) {
                const accountIcon = document.getElementById('wcAccountIcon');
                const accountValue = document.getElementById('wcCurrentAccount');
                if (accountIcon) accountIcon.textContent = '🔒';
                if (accountValue) accountValue.textContent = wcCaptchaLastAccount || '已登录';
                saShowToast('账号状态已更新', 'success');
            } else {
                saShowToast('保持未登录状态', 'info');
            }
            closeWcCaptchaDialog();
        } else {
            setWcCaptchaStatus('验证码提交失败: ' + (data.message || '未知错误'));
            saShowToast(data.message || '验证码提交失败', 'error');
        }
    } catch (e) {
        setWcCaptchaStatus('验证码提交异常: ' + e.message);
        saShowToast('验证码提交失败: ' + e.message, 'error');
    }
}

// 关闭截图
// 关闭截图显示
function closeScreenshot() {
    const areaEl = document.getElementById('wcScreenshotArea');
    if (areaEl) areaEl.style.display = 'none';
}

// 获取系统状态
// 获取系统状态
async function getWorkerSystemStatus() {
    if (!currentRemoteWorkerId) {
        saShowToast('请先选择服务器', 'error');
        return;
    }

    appendWorkerConsole('正在获取系统状态...', 'command');

    try {
        const response = await fetch(`${API_BASE_URL}/super-admin/worker/${encodeURIComponent(currentRemoteWorkerId)}/control`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ action: 'system_status', params: {} })
        });

        const data = await response.json();

        if (data.success) {
            appendWorkerConsole('状态查询命令已发送...', 'info');

            setTimeout(async () => {
                try {
                    const statusResponse = await fetch(`${API_BASE_URL}/super-admin/worker/${encodeURIComponent(currentRemoteWorkerId)}/system-status`);
                    const statusData = await statusResponse.json();

                    if (statusData.success && statusData.data) {
                        const s = statusData.data;
                        const contentEl = document.getElementById('wcSystemStatusContent');
                        const areaEl = document.getElementById('wcSystemStatusArea');

                        if (contentEl && areaEl) {
                            // 系统状态信息
                            let sysStatusHtml = `
                            <div style="display:grid; grid-template-columns:1fr 1fr; gap:8px;">
                                <div>CPU: <span style="color:${s.cpu_percent > 80 ? '#ff5252' : '#00ff88'}">${s.cpu_percent}%</span></div>
                                <div>内存: <span style="color:${s.memory_percent > 80 ? '#ff5252' : '#00ff88'}">${s.memory_percent}%</span></div>
                                <div>内存用量: <span style="color:#4facfe">${s.memory_used} / ${s.memory_total} GB</span></div>
                                <div>磁盘: <span style="color:${s.disk_percent > 80 ? '#ff5252' : '#00ff88'}">${s.disk_percent}%</span></div>
                                <div>可用空间: <span style="color:#4facfe">${s.disk_free} GB</span></div>
                                <div>运行时长: <span style="color:#ffd93d">${s.uptime_hours} 小时</span></div>
                            </div>
                            <div style="margin-top:10px; padding-top:10px; border-top:1px solid rgba(255,255,255,0.1);">
                                <div>平台: <span style="color:#888">${s.platform}</span></div>
                                <div>Python: <span style="color:#888">${s.python_version}</span></div>
                            </div>
                        `;

                            // 任务统计信息
                            if (s.stats) {
                                const st = s.stats;
                                const totalSent = (st.success || 0) + (st.failed || 0);
                                const rate = totalSent > 0 ? Math.round((st.success || 0) / totalSent * 100) : 0;
                                sysStatusHtml += `
                                <div style="margin-top:15px; padding-top:10px; border-top:1px solid rgba(255,255,255,0.2);">
                                    <div style="font-weight:bold; margin-bottom:8px; color:#ffd93d;">📊 任务统计</div>
                                    <div style="display:grid; grid-template-columns:1fr 1fr; gap:8px;">
                                        <div>Shards: <span style="color:#00ff88">${st.shards || 0}</span></div>
                                        <div>已发送: <span style="color:#4facfe">${st.sent || 0}</span></div>
                                        <div>成功: <span style="color:#00ff88">${st.success || 0}</span></div>
                                        <div>失败: <span style="color:#ff5252">${st.failed || 0}</span></div>
                                        <div>成功率: <span style="color:${rate > 80 ? '#00ff88' : '#ffd93d'}">${rate}%</span></div>
                                    </div>
                                </div>
                            `;
                            }

                            contentEl.innerHTML = sysStatusHtml;
                            areaEl.style.display = 'block';
                            appendWorkerConsole('系统状态已加载', 'success');
                        }
                    } else {
                        appendWorkerConsole('获取状态失败，请重试', 'warning');
                    }
                } catch (e) {
                    appendWorkerConsole('获取状态失败: ' + e.message, 'error');
                }
            }, 1500);
        } else {
            appendWorkerConsole('查询失败: ' + data.message, 'error');
        }
    } catch (e) {
        appendWorkerConsole('查询失败: ' + e.message, 'error');
    }
}

// 关闭系统状态
// 关闭系统状态
function closeSystemStatus() {
    const areaEl = document.getElementById('wcSystemStatusArea');
    if (areaEl) areaEl.style.display = 'none';
}

// 设置Worker信息值
// 设置Worker信息值
function setWorkerInfoValue(id, value) {
    const el = document.getElementById(id);
    if (el) {
        el.textContent = value;
    }
}

// 向控制台添加日志行
// 向控制台添加日志
function appendWorkerConsole(message, type = 'info') {
    const consoleEl = document.getElementById('workerConsole');
    if (!consoleEl) {
        return;
    }

    const timestamp = new Date().toLocaleTimeString('zh-CN');
    const line = document.createElement('div');
    line.className = `console-line ${type}`;
    line.innerHTML = `<span class="timestamp">${timestamp}</span> ${message}`;

    consoleEl.appendChild(line);
    consoleEl.scrollTop = consoleEl.scrollHeight;
}

// 清空控制台
// 清空控制台
function clearWorkerConsole() {
    const consoleEl = document.getElementById('workerConsole');
    if (consoleEl) {
        consoleEl.innerHTML = `
        <div class="console-line info">
            <span class="timestamp">${new Date().toLocaleTimeString('zh-CN')}</span>
            Console cleared. Ready for commands.
        </div>
    `;
    }
}

// 监听点击事件关闭面板
document.addEventListener('click', function (e) {
    const overlay = document.getElementById('workerRemotePanelOverlay');
    if (overlay && overlay.classList.contains('show') && e.target === overlay) {
        closeWorkerRemotePanel();
    }
});

// 键盘快捷键
document.addEventListener('keydown', function (e) {
    if (e.key === 'Escape') {
        closeWorkerRemotePanel();
        closeWcSystemDialog();
        closeWcLogoutDialog();
    }
});

// 显示系统状态弹窗
// 显示系统状态弹窗
async function showWcSystemStatus() {
    const dialog = document.getElementById('wcSysDialog');
    const content = document.getElementById('wcSysDialogContent');

    if (!dialog || !content) return;

    dialog.style.display = 'flex';
    content.innerHTML = '<div class="wc-sys-loading"><div class="loading-spinner"></div><span>正在获取系统信息...</span></div>';

    if (!currentRemoteWorkerId) {
        content.innerHTML = '<div style="text-align:center;color:#ff5252;padding:20px;">请先选择服务器</div>';
        return;
    }

    try {
        const response = await fetch(`${API_BASE_URL}/super-admin/worker/${encodeURIComponent(currentRemoteWorkerId)}/control`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ action: 'system_status', params: {} })
        });

        const data = await response.json();

        if (data.success) {
            // Check if system status data is available in the immediate response
            if (data.data && data.data.system_status) {
                const s = data.data.system_status;
                content.innerHTML = `
                <div class="wc-sys-grid">
                    <div class="wc-sys-item">
                        <span class="item-label">CPU</span>
                        <span class="item-value ${s.cpu_percent > 80 ? 'danger' : 'good'}">${s.cpu_percent || 0}%</span>
                    </div>
                    <div class="wc-sys-item">
                        <span class="item-label">内存</span>
                        <span class="item-value ${s.memory_percent > 80 ? 'danger' : 'good'}">${s.memory_percent || 0}%</span>
                    </div>
                    <div class="wc-sys-item wc-sys-full">
                        <span class="item-label">内存用量</span>
                        <span class="item-value info">${s.memory_used || 0} / ${s.memory_total || 0} GB</span>
                    </div>
                    <div class="wc-sys-item">
                        <span class="item-label">磁盘</span>
                        <span class="item-value ${s.disk_percent > 80 ? 'danger' : 'good'}">${s.disk_percent || 0}%</span>
                    </div>
                    <div class="wc-sys-item">
                        <span class="item-label">可用空间</span>
                        <span class="item-value info">${s.disk_free || 0} GB</span>
                    </div>
                    <div class="wc-sys-divider"></div>
                    <div class="wc-sys-item">
                        <span class="item-label">运行时长</span>
                        <span class="item-value warning">${s.uptime_hours || 0} 小时</span>
                    </div>
                    <div class="wc-sys-item wc-sys-full">
                        <span class="item-label">平台</span>
                        <span class="item-value">${s.platform || 'Unknown'}</span>
                    </div>
                    <div class="wc-sys-item wc-sys-full">
                        <span class="item-label">Python</span>
                        <span class="item-value">${s.python_version || 'Unknown'}</span>
                    </div>
                </div>
            `;
                appendWorkerConsole('系统状态已加载', 'success');
                return;
            }

            // Wait a moment for the system status to be processed
            await new Promise(resolve => setTimeout(resolve, 1500));

            if (!currentRemoteWorkerId) return;

            // Check if status data is in the original response
            if (data.system_status) {
                const s = data.system_status;
                content.innerHTML = `
                <div class="wc-sys-grid">
                    <div class="wc-sys-item">
                        <span class="item-label">CPU</span>
                        <span class="item-value ${s.cpu_percent > 80 ? 'danger' : 'good'}">${s.cpu_percent || 0}%</span>
                    </div>
                    <div class="wc-sys-item">
                        <span class="item-label">内存</span>
                        <span class="item-value ${s.memory_percent > 80 ? 'danger' : 'good'}">${s.memory_percent || 0}%</span>
                    </div>
                    <div class="wc-sys-item wc-sys-full">
                        <span class="item-label">内存用量</span>
                        <span class="item-value info">${s.memory_used || 0} / ${s.memory_total || 0} GB</span>
                    </div>
                    <div class="wc-sys-item">
                        <span class="item-label">磁盘</span>
                        <span class="item-value ${s.disk_percent > 80 ? 'danger' : 'good'}">${s.disk_percent || 0}%</span>
                    </div>
                    <div class="wc-sys-item">
                        <span class="item-label">可用空间</span>
                        <span class="item-value info">${s.disk_free || 0} GB</span>
                    </div>
                    <div class="wc-sys-divider"></div>
                    <div class="wc-sys-item">
                        <span class="item-label">运行时长</span>
                        <span class="item-value warning">${s.uptime_hours || 0} 小时</span>
                    </div>
                    <div class="wc-sys-item wc-sys-full">
                        <span class="item-label">平台</span>
                        <span class="item-value">${s.platform || 'Unknown'}</span>
                    </div>
                    <div class="wc-sys-item wc-sys-full">
                        <span class="item-label">Python</span>
                        <span class="item-value">${s.python_version || 'Unknown'}</span>
                    </div>
                </div>
            `;
                appendWorkerConsole('系统状态已加载', 'success');
                return;
            }

            // If no direct data, try a GET request with action parameter
            try {
                const statusResponse = await fetch(`${API_BASE_URL}/super-admin/worker/${encodeURIComponent(currentRemoteWorkerId)}/control?action=system_status`, {
                    method: 'GET',
                    headers: { 'Content-Type': 'application/json' }
                });
                const statusData = await statusResponse.json();

                if (statusData.success && statusData.data) {
                    const s = statusData.data;
                    content.innerHTML = `
                    <div class="wc-sys-grid">
                        <div class="wc-sys-item">
                            <span class="item-label">CPU</span>
                            <span class="item-value ${s.cpu_percent > 80 ? 'danger' : 'good'}">${s.cpu_percent || 0}%</span>
                        </div>
                        <div class="wc-sys-item">
                            <span class="item-label">内存</span>
                            <span class="item-value ${s.memory_percent > 80 ? 'danger' : 'good'}">${s.memory_percent || 0}%</span>
                        </div>
                        <div class="wc-sys-item wc-sys-full">
                            <span class="item-label">内存用量</span>
                            <span class="item-value info">${s.memory_used || 0} / ${s.memory_total || 0} GB</span>
                        </div>
                        <div class="wc-sys-item">
                            <span class="item-label">磁盘</span>
                            <span class="item-value ${s.disk_percent > 80 ? 'danger' : 'good'}">${s.disk_percent || 0}%</span>
                        </div>
                        <div class="wc-sys-item">
                            <span class="item-label">可用空间</span>
                            <span class="item-value info">${s.disk_free || 0} GB</span>
                        </div>
                        <div class="wc-sys-divider"></div>
                        <div class="wc-sys-item">
                            <span class="item-label">运行时长</span>
                            <span class="item-value warning">${s.uptime_hours || 0} 小时</span>
                        </div>
                        <div class="wc-sys-item wc-sys-full">
                            <span class="item-label">平台</span>
                            <span class="item-value">${s.platform || 'Unknown'}</span>
                        </div>
                        <div class="wc-sys-item wc-sys-full">
                            <span class="item-label">Python</span>
                            <span class="item-value">${s.python_version || 'Unknown'}</span>
                        </div>
                    </div>
                `;
                    appendWorkerConsole('系统状态已加载', 'success');
                } else {
                    content.innerHTML = '<div style="text-align:center;color:#ffd93d;padding:20px;">获取状态失败: ' + (statusData.message || '未知错误') + '</div>';
                }
            } catch (e) {
                content.innerHTML = '<div style="text-align:center;color:#ff5252;padding:20px;">获取状态失败，请稍后重试</div>';
            }
        } else {
            content.innerHTML = '<div style="text-align:center;color:#ff5252;padding:20px;">查询失败: ' + (data.message || '未知错误') + '</div>';
        }
    } catch (e) {
        content.innerHTML = '<div style="text-align:center;color:#ff5252;padding:20px;">网络错误: ' + e.message + '</div>';
    }
}

// 关闭系统状态弹窗
// 关闭系统状态弹窗
function closeWcSystemDialog() {
    const dialog = document.getElementById('wcSysDialog');
    if (dialog) dialog.style.display = 'none';
}

// 显示退出确认弹窗
// 显示退出确认弹窗
function confirmWcLogout() {
    const dialog = document.getElementById('wcLogoutDialog');
    if (dialog) dialog.style.display = 'flex';
}

// 关闭退出确认弹窗
// 关闭退出确认弹窗
function closeWcLogoutDialog() {
    const dialog = document.getElementById('wcLogoutDialog');
    if (dialog) dialog.style.display = 'none';
}

// 执行退出登录
// 执行退出登录
async function confirmWcLogoutExecute() {
    closeWcLogoutDialog();
    await wcLogout();
}

//#endregion
