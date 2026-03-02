//#region 服务器管理模块全局变量
let serverData = { connected: [], disconnected: [] };
try {
    const savedServerData = StorageManager.server.getServerData();
    if (savedServerData) { StorageManager.server.clearServerData(); }
} catch (error) { }
let adminAccounts = [];
//#endregion

//#region 管理员账户管理
/** 去重管理员账户列表 */
function _dedupeAdminAccounts(list) {
    const map = new Map();
    (Array.isArray(list) ? list : []).forEach(a => {
        if (!a || !a.id) return;
        const key = String(a.id).trim();
        if (!key) return;
        const prev = map.get(key) || {};
        map.set(key, {
            id: key,
            selectedServers: Array.isArray(a.selectedServers) ? a.selectedServers : (Array.isArray(prev.selectedServers) ? prev.selectedServers : []),
            userGroups: Array.isArray(a.userGroups) ? a.userGroups : (Array.isArray(prev.userGroups) ? prev.userGroups : undefined),
        });
    });
    return Array.from(map.values());
}

/** 从API加载管理员账户列表 */
async function loadAdminAccountsFromAPI() {
    try {
        const token = StorageManager.session.getServerManagerToken();
        const headers = token ? { 'Authorization': `Bearer ${token}` } : {};
        const resp = await fetch(`${API_BASE_URL}/admin/account?t=${Date.now()}`, { method: 'GET', headers: headers });
        const data = await resp.json().catch(() => ({}));
        if (resp.ok && data.success && Array.isArray(data.admins)) {
            const localAccounts = new Map((_dedupeAdminAccounts(adminAccounts)).map(a => [a.id, a]));
            let deletedIds = StorageManager.admin.getDeletedAdminIds();
            data.admins.forEach(row => {
                const id = String((row && row.admin_id) || '').trim();
                if (!id) return;
                if (deletedIds.includes(id)) return;
                if (!localAccounts.has(id)) { localAccounts.set(id, { id, selectedServers: [] }); }
            });
            adminAccounts = _dedupeAdminAccounts(Array.from(localAccounts.values()));
        }
    } catch (e) { }
}

/** 静默调试日志(空实现) */
function silentDebugLog(data) { }

/** 带超时的fetch请求 */
async function fetchWithTimeout(url, options = {}, timeout = 60000) {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), timeout);
    try {
        const response = await fetch(url, { ...options, signal: controller.signal });
        clearTimeout(timeoutId);
        return response;
    } catch (error) {
        clearTimeout(timeoutId);
        if (error.name === 'AbortError') { throw new Error(`请求超时 (${timeout / 1000}秒)`); }
        throw error;
    }
}

/** 测试API连接是否正常 */
async function testAPIConnection() {
    try {
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 10000);
        const response = await fetch(`${API_BASE_URL}/servers`, { method: 'GET', headers: { 'Content-Type': 'application/json' }, signal: controller.signal });
        clearTimeout(timeoutId);
        return { success: true, status: response.status };
    } catch (error) {
        return { success: false, error: error.message, details: { name: error.name, message: error.message, type: error.name === 'AbortError' ? 'timeout' : error.message.includes('Failed to fetch') ? 'network' : error.message.includes('CORS') ? 'cors' : 'unknown' } };
    }
}
//#endregion

//#region 服务器列表加载
let _serversLoadedOnce = false;
let _serversLoading = false;
let _lastServersLoadTime = 0;
const SERVERS_LOAD_MIN_INTERVAL = 1000;

/** 从API加载服务器列表 */
async function loadServersFromAPI() {
    const now = Date.now();
    if (_serversLoading) { return; }
    if (now - _lastServersLoadTime < SERVERS_LOAD_MIN_INTERVAL) { return; }
    _serversLoading = true;
    _lastServersLoadTime = now;
    try {
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 30000);
        const response = await fetch(`${API_BASE_URL}/servers?t=${Date.now()}`, { method: 'GET', headers: { 'Content-Type': 'application/json' }, signal: controller.signal });
        clearTimeout(timeoutId);
        if (!response.ok) { throw new Error(`API响应错误: ${response.status} ${response.statusText}`); }
        const data = await response.json();
        if (data.success && data.servers) {
            serverData.connected = [];
            serverData.disconnected = [];
            const serverMap = new Map();
            data.servers.forEach(s => {
                const server_id = s.server_id;
                if (!server_id) return;
                if (serverMap.has(server_id)) {
                    const existing = serverMap.get(server_id);
                    const newStatus = (s.status || '').toLowerCase();
                    if (newStatus === 'connected' || newStatus === 'available' || newStatus === 'ready') {
                        if (existing.status !== 'connected' && existing.status !== 'available' && existing.status !== 'ready') {
                            serverMap.set(server_id, { name: s.server_name || s.server_id, url: s.server_url || '', server_id: server_id, status: (newStatus === 'available' || newStatus === 'ready') ? 'connected' : newStatus, assigned_user_id: s.assigned_user_id || null, last_seen: s.last_seen });
                        }
                    }
                } else {
                    const serverItem = { name: s.server_name || s.server_id, url: s.server_url || '', server_id: server_id, status: (s.status || '').toLowerCase(), assigned_user_id: s.assigned_user_id || null, last_seen: s.last_seen };
                    if (serverItem.status === 'available' || serverItem.status === 'ready') { serverItem.status = 'connected'; }
                    serverMap.set(server_id, serverItem);
                }
            });
            serverMap.forEach(server => {
                if (server.status === 'connected' || server.status === 'available' || server.status === 'ready') { serverData.connected.push(server); } else { serverData.disconnected.push(server); }
            });
            _serversLoadedOnce = true;
            const realNames = new Set([...serverData.connected].map(s => String(s.name || '').trim()).filter(Boolean));
            let cleaned = false;
            try {
                adminAccounts.forEach(acc => {
                    if (Array.isArray(acc.selectedServers)) {
                        const before = acc.selectedServers.length;
                        acc.selectedServers = acc.selectedServers.filter(n => realNames.has(String(n).trim()));
                        if (acc.selectedServers.length !== before) cleaned = true;
                    }
                    if (Array.isArray(acc.userGroups)) {
                        acc.userGroups.forEach(g => {
                            if (Array.isArray(g.servers)) {
                                const b = g.servers.length;
                                g.servers = g.servers.filter(n => realNames.has(String(n).trim()));
                                if (g.servers.length !== b) cleaned = true;
                            }
                        });
                    }
                });
                if (Array.isArray(managerUserGroups)) {
                    managerUserGroups.forEach(g => {
                        if (Array.isArray(g.servers)) {
                            const b = g.servers.length;
                            g.servers = g.servers.filter(n => realNames.has(String(n).trim()));
                            if (g.servers.length !== b) cleaned = true;
                        }
                    });
                }
            } catch (e) { }
            updateServerDisplay();
            if (document.getElementById('adminManageServersGrid')) { }
        } else {
        }
    } catch (error) {
    } finally { _serversLoading = false; }
}
//#endregion

//#region 专属号码管理
let exclusivePhoneNumbers = [];
let currentSelectedPhone = null;

/** 加载用户专属号码列表 */
async function loadExclusivePhoneNumbers() {
    if (!window.currentUserId) { document.getElementById('exclusivePhoneSelector').style.display = 'none'; return; }
    try {
        const response = await fetch(`${API_BASE_URL}/users/${window.currentUserId}/available-servers`);
        if (response.ok) {
            const data = await response.json();
            if (data.success && data.exclusive_servers && data.exclusive_servers.length > 0) {
                exclusivePhoneNumbers = [];
                for (const server of data.exclusive_servers) {
                    const phoneNumber = server.phone_number || server.server_name || server.server_id;
                    if (phoneNumber && !exclusivePhoneNumbers.find(p => p.phone === phoneNumber)) {
                        exclusivePhoneNumbers.push({ phone: phoneNumber, server_id: server.server_id, server_name: server.server_name });
                    }
                }
                if (exclusivePhoneNumbers.length > 0) {
                    document.getElementById('exclusivePhoneSelector').style.display = 'block';
                    if (!currentSelectedPhone && exclusivePhoneNumbers.length > 0) { currentSelectedPhone = exclusivePhoneNumbers[0].phone; }
                    updateExclusivePhoneDisplay();
                } else { document.getElementById('exclusivePhoneSelector').style.display = 'none'; }
            } else { document.getElementById('exclusivePhoneSelector').style.display = 'none'; }
        } else { document.getElementById('exclusivePhoneSelector').style.display = 'none'; }
    } catch (error) { document.getElementById('exclusivePhoneSelector').style.display = 'none'; }
}

/** 更新专属号码下拉显示 */
function updateExclusivePhoneDisplay() {
    const currentPhoneDisplay = document.getElementById('currentPhoneDisplay');
    const dropdown = document.getElementById('exclusivePhoneDropdown');
    const btn = document.getElementById('exclusivePhoneBtn');
    if (currentSelectedPhone && currentPhoneDisplay) { currentPhoneDisplay.textContent = currentSelectedPhone; }
    if (dropdown) {
        dropdown.innerHTML = '';
        exclusivePhoneNumbers.forEach(item => {
            const option = document.createElement('div');
            option.style.cssText = 'padding: 8px 12px; cursor: pointer; border-bottom: 1px solid #eee; transition: background 0.2s;';
            if (item.phone === currentSelectedPhone) { option.style.background = 'rgba(76, 175, 80, 0.2)'; option.style.fontWeight = 'bold'; }
            option.textContent = item.phone;
            option.onclick = () => { currentSelectedPhone = item.phone; updateExclusivePhoneDisplay(); dropdown.style.display = 'none'; loadInboxForPhone(item.phone); };
            option.onmouseenter = () => { if (item.phone !== currentSelectedPhone) { option.style.background = 'rgba(76, 175, 80, 0.1)'; } };
            option.onmouseleave = () => { if (item.phone !== currentSelectedPhone) { option.style.background = 'transparent'; } };
            dropdown.appendChild(option);
        });
    }
    if (btn) {
        if (exclusivePhoneNumbers.length === 1) {
            btn.innerHTML = `本机号码: <span id="currentPhoneDisplay">${currentSelectedPhone || '-'}</span>`;
            btn.onclick = null;
            btn.style.cursor = 'default';
        } else {
            btn.innerHTML = `本机号码: <span id="currentPhoneDisplay">${currentSelectedPhone || '-'} <span style="font-size: 10px;">▼</span></span>`;
            btn.onclick = (e) => { e.stopPropagation(); const dropdown = document.getElementById('exclusivePhoneDropdown'); if (dropdown) { const isVisible = dropdown.style.display === 'block'; dropdown.style.display = isVisible ? 'none' : 'block'; } };
            btn.style.cursor = 'pointer';
        }
    }
}

/** 加载指定号码的收件箱(空实现) */
function loadInboxForPhone(phoneNumber) { }

//#endregion

//#region 认证与连接
/** 检查用户认证Token是否有效 */
function checkAuthToken() {
    if (typeof StorageManager === 'undefined' || !StorageManager.session) { return null; }
    if (StorageManager.session.isSessionExpired && StorageManager.session.isSessionExpired()) { return null; }
    const token = StorageManager.session.getUserToken();
    if (token) { window.authToken = token; }
    return token || null;
}

/** 连接到分配给用户的服务器 */
async function connectToAssignedServers() {
    window.authToken = window.authToken || checkAuthToken();
    if (!window.authToken) return;
    connectToBackendWS(null);
}

// 如果有token则自动连接
if (checkAuthToken()) { connectToAssignedServers(); }
//#endregion

//#region 收件箱轮询
let inboxPollingInterval = null;

/** 启动收件箱定时轮询(空实现) */
function startInboxPolling(userId) { }

/** 停止收件箱轮询 */
function stopInboxPolling() {
    if (inboxPollingInterval) { clearInterval(inboxPollingInterval); inboxPollingInterval = null; }
}

/** 停止服务器轮询(空实现) */
function stopServerPolling() { }

/** 轮询收件箱(空实现) */
async function pollInbox(userId) { return; }

/** 加载会话消息 */
async function loadConversationMessages(chatId) { requestConversation(chatId); }
//#endregion

//#region 服务器删除
/** 显示删除服务器确认弹窗 */
async function showDeleteServerConfirm(serverName) {
    return new Promise((resolve) => {
        const modal = document.createElement('div');
        modal.className = 'custom-modal-overlay';
        modal.id = 'deleteServerConfirmModal';
        modal.style.display = 'flex';
        modal.innerHTML = `
            <div class="custom-modal-panel" style="width: 380px;">
                <div class="custom-modal-header">
                    <span class="custom-modal-title">⚠️ 删除服务器记录</span>
                    <button class="custom-modal-close" onclick="this.closest('.custom-modal-overlay').remove(); resolve(false);">×</button>
                </div>
                <div class="custom-modal-content">
                    <div class="custom-modal-message" style="text-align: center; padding: 10px 0;">
                        确定要删除服务器 <strong style="color: #ff4757;">${serverName}</strong> 的记录吗？<br>
                        <span style="font-size: 12px; color: #666; margin-top: 8px; display: block;">删除历史服务器</span>
                    </div>
                    <div class="custom-modal-buttons">
                        <button class="custom-modal-btn cancel" onclick="this.closest('.custom-modal-overlay').remove(); resolve(false);">取消</button>
                        <button class="custom-modal-btn confirm" onclick="this.closest('.custom-modal-overlay').remove(); resolve(true);" style="background: linear-gradient(135deg, #ff4757 0%, #ff3838 100%); color: white;">确认删除</button>
                    </div>
                </div>
            </div>`;
        document.body.appendChild(modal);
        setTimeout(() => modal.classList.add('show'), 10);
        modal.addEventListener('click', (e) => { if (e.target === modal) { modal.classList.remove('show'); setTimeout(() => { modal.remove(); resolve(false); }, 150); } });
        const cancelBtn = modal.querySelector('.custom-modal-btn.cancel');
        const confirmBtn = modal.querySelector('.custom-modal-btn.confirm');
        cancelBtn.onclick = () => { modal.classList.remove('show'); setTimeout(() => { modal.remove(); resolve(false); }, 150); };
        confirmBtn.onclick = () => { modal.classList.remove('show'); setTimeout(() => { modal.remove(); resolve(true); }, 150); };
    });
}

/** 删除服务器记录(API) */
async function deleteServer(serverId) {
    try {
        const response = await fetch(`${API_BASE_URL}/servers/${serverId}`, { method: 'DELETE', headers: { 'Content-Type': 'application/json' } });
        const result = await response.json();
        if (result.success) {
            await customAlert(`服务器记录已删除`);
            await loadServersFromAPI();
            if (typeof updateServerDisplay === 'function') { updateServerDisplay(); }
            if (window.activeWs && window.activeWs.readyState === WebSocket.OPEN) { window.activeWs.send(JSON.stringify({ action: 'get_servers' })); }
        } else { await customAlert(`删除失败: ${result.message || '未知错误'}`); }
    } catch (error) { await customAlert(`删除失败: ${error.message}`); }
}

/** 断开服务器连接(API) */
async function disconnectServer(serverId) {
    try {
        const response = await fetch(`${API_BASE_URL}/servers/${serverId}/disconnect`, { method: 'POST', headers: { 'Content-Type': 'application/json' } });
        if (response.ok) {
            const data = await response.json();
            if (data.success) { showMessage('服务器已断开连接', 'success'); await loadServersFromAPI(); } else { await customAlert('断开连接失败: ' + (data.message || '未知错误')); }
        } else { const error = await response.json(); await customAlert('断开连接失败: ' + (error.message || '网络错误')); }
    } catch (error) { await customAlert('断开连接失败: ' + error.message); }
}

if (StorageManager.session.getUserId()) { connectToAssignedServers(); }
//#endregion

//#region 服务器显示更新
let updateServerDisplayTimer = null;

/** 获取服务器状态文本 */
function getServerStatusText(button) {
    if (button.classList.contains('connected')) {
        if (button.classList.contains('private') || button.classList.contains('active')) { return '状态: 正在使用'; }
        else if (button.classList.contains('selected')) { return '状态: 已选中'; }
        else { return '状态: 已连接'; }
    } else if (button.classList.contains('disconnected')) { return '状态: 断开连接'; }
    else if (button.classList.contains('selected')) { return '状态: 已选中'; }
    else if (button.classList.contains('private') || button.classList.contains('active')) { return '状态: 正在使用'; }
    return '状态: 未知';
}

/** 更新服务器显示列表 */
function updateServerDisplay() {
    if (updateServerDisplayTimer) { clearTimeout(updateServerDisplayTimer); }
    updateServerDisplayTimer = setTimeout(() => {
        const connectedContainer = document.getElementById('connectedServers');
        const disconnectedContainer = document.getElementById('disconnectedServers');
        if (!connectedContainer && !disconnectedContainer) { return; }
        if (!serverData) { serverData = { connected: [], disconnected: [] }; }
        if (!Array.isArray(serverData.connected)) { serverData.connected = []; }
        if (!Array.isArray(serverData.disconnected)) { serverData.disconnected = []; }
        const serverExclusiveMap = new Map();
        [...serverData.connected, ...(serverData.disconnected || [])].forEach(server => { if (server.assigned_user_id) { serverExclusiveMap.set(server.name, server.assigned_user_id); } });
        adminAccounts.forEach(account => { if (account.userGroups) { account.userGroups.forEach(group => { if (group.servers) { group.servers.forEach(serverName => { if (!serverExclusiveMap.has(serverName)) { serverExclusiveMap.set(serverName, group.userId); } }); } }); } });
        const getExclusiveInfo = (serverName) => { const userId = serverExclusiveMap.get(serverName); if (userId) { const userIdOnly = userId.startsWith('u_') ? userId.substring(2) : userId; return { isExclusive: true, displayName: serverName, userIdDisplay: userIdOnly }; } return { isExclusive: false, displayName: serverName }; };
        if (connectedContainer) {
            const connectedFragment = document.createDocumentFragment();
            connectedContainer.innerHTML = '';
            serverData.connected.forEach(server => {
                const btn = document.createElement('button');
                btn.className = 'server-button connected';
                const exclusiveInfo = getExclusiveInfo(server.name);
                const portMatch = (server.url || '').match(/:(\d+)/);
                const port = portMatch ? portMatch[1] : (server.port || (server.name || '').match(/\d+/)?.[0] || '?');
                const botHTML = SERVER_BOT_HTML;
                btn.innerHTML = botHTML + `
                    <div class="server-button-name" style="position: absolute; bottom: -20px; left: 50%; transform: translateX(-50%); font-size: 11px; color: #2d3436; white-space: nowrap; pointer-events: none; z-index: 100;">${port}</div>
                    <div class="server-tooltip">
                        <div style="font-weight: bold; margin-bottom: 4px;">${server.name}</div>
                        <div style="font-size: 11px; opacity: 0.9;">${server.url || ''}</div>
                        <div style="font-size: 11px; color: #00ff88; margin-top: 4px;" class="status-text">状态: 已连接</div>
                        ${exclusiveInfo.isExclusive ? `<div style="font-size: 11px; color: #ff6b6b; margin-top: 2px;">私享服务器:${exclusiveInfo.userIdDisplay}</div>` : ''}
                    </div>`;
                if (exclusiveInfo.isExclusive) { btn.classList.add('private'); }
                connectedFragment.appendChild(btn);
            });
            connectedContainer.appendChild(connectedFragment);
            if (typeof initRadarBots === 'function') initRadarBots();
            const countEl = document.getElementById('connectedCount');
            if (countEl) countEl.textContent = `(${serverData.connected.length})`;
        } else if (connectedContainer) {
            connectedContainer.innerHTML = '';
            const countEl = document.getElementById('connectedCount');
            if (countEl) countEl.textContent = '(0)';
        }
        if (disconnectedContainer) {
            const disconnectedFragment = document.createDocumentFragment();
            disconnectedContainer.innerHTML = '';
            serverData.disconnected.forEach(server => {
                const btn = document.createElement('button');
                btn.className = 'server-button disconnected';
                const portMatch = (server.url || '').match(/:(\d+)/);
                const port = portMatch ? portMatch[1] : (server.port || (server.name || '').match(/\d+/)?.[0] || '?');
                const botHTML = SERVER_BOT_HTML;
                btn.innerHTML = botHTML + `
                    <div class="server-button-name" style="position: absolute; bottom: -20px; left: 50%; transform: translateX(-50%); font-size: 11px; color: #2d3436; white-space: nowrap; pointer-events: none; z-index: 100;">${port}</div>
                    <div class="server-tooltip">
                        <div style="font-weight: bold; margin-bottom: 4px;">${server.name}</div>
                        <div style="font-size: 11px; opacity: 0.9;">${server.url || ''}</div>
                        <div style="font-size: 11px; color: #888888; margin-top: 4px;" class="status-text">状态: 断开                    </div>`;
                btn.onclick = () => { btn.classList.toggle('active'); const statusText = btn.querySelector('.status-text'); if (statusText) { statusText.textContent = getServerStatusText(btn); } };
                const deleteBtn = document.createElement('button');
                deleteBtn.className = 'server-delete-btn';
                deleteBtn.innerHTML = '×';
                deleteBtn.title = '删除服务器记录';
                deleteBtn.onclick = async (e) => { e.stopPropagation(); if (await showDeleteServerConfirm(server.name)) { await deleteServer(server.server_id); } };
                btn.appendChild(deleteBtn);
                disconnectedFragment.appendChild(btn);
            });
            disconnectedContainer.appendChild(disconnectedFragment);
            if (typeof initRadarBots === 'function') initRadarBots();
            const disconnectedCountEl = document.getElementById('disconnectedCount');
            if (disconnectedCountEl) disconnectedCountEl.textContent = `(${serverData.disconnected.length})`;
        } else if (disconnectedContainer) {
            disconnectedContainer.innerHTML = '';
            const disconnectedCountEl = document.getElementById('disconnectedCount');
            if (disconnectedCountEl) disconnectedCountEl.textContent = '(0)';
        }
    }, 50);
}
//#endregion

//#region 管理员账户弹窗
/** 显示添加管理员弹窗 */
function showAddAdminModal() {
    const modal = document.getElementById('addAdminModal');
    if (!modal) return;
    const idInput = document.getElementById('newAdminId');
    const passwordInput = document.getElementById('newAdminPassword');
    idInput.value = '';
    passwordInput.value = '';
    const idHandler = idInput._enterHandler;
    const passwordHandler = passwordInput._enterHandler;
    if (idHandler) idInput.removeEventListener('keypress', idHandler);
    if (passwordHandler) passwordInput.removeEventListener('keypress', passwordHandler);
    const idEnterHandler = (e) => { if (e.key === 'Enter') { e.preventDefault(); passwordInput.focus(); } };
    const passwordEnterHandler = (e) => { if (e.key === 'Enter') { e.preventDefault(); addAdminAccount(); } };
    idInput.addEventListener('keypress', idEnterHandler);
    passwordInput.addEventListener('keypress', passwordEnterHandler);
    idInput._enterHandler = idEnterHandler;
    passwordInput._enterHandler = passwordEnterHandler;
    requestAnimationFrame(() => { modal.classList.add('show'); setTimeout(() => { idInput.focus(); }, 50); });
}

/** 关闭添加管理员弹窗 */
function closeAddAdminModal() {
    const modal = document.getElementById('addAdminModal');
    if (!modal) return;
    modal.classList.remove('show');
    setTimeout(() => { document.getElementById('newAdminId').value = ''; document.getElementById('newAdminPassword').value = ''; }, 150);
}

/** 添加管理员账户(API) */
async function addAdminAccount() {
    const id = document.getElementById('newAdminId').value.trim();
    const password = document.getElementById('newAdminPassword').value.trim();
    if (!id || !password) { await customAlert('请填写管理员ID和密码'); return; }
    if (adminAccounts.some(a => a.id === id)) { closeAddAdminModal(); setTimeout(async () => { await customAlert('该管理员ID已存在'); }, 300); return; }
    try {
        const token = StorageManager.session.getServerManagerToken();
        const headers = { 'Content-Type': 'application/json' };
        if (token) { headers['Authorization'] = `Bearer ${token}`; }
        const response = await fetch(`${API_BASE_URL}/admin/account`, { method: 'POST', headers: headers, body: JSON.stringify({ admin_id: id, password: password }) });
        if (!response.ok) { const errorData = await response.json().catch(() => ({})); closeAddAdminModal(); setTimeout(async () => { await customAlert(`保存失败：${errorData.message || response.statusText || '未知错误'}`); }, 300); return; }
        adminAccounts.push({ id, selectedServers: [] });
    } catch (error) { closeAddAdminModal(); setTimeout(async () => { await customAlert('无法连接到API保存管理员账号（未写入数据库），本次不会保存到本地以免造成不同步'); }, 300); return; }
    closeAddAdminModal();
    setTimeout(async () => { await customAlert('管理员账号已添加'); updateAdminAccountDisplay(); }, 150);
}

/** 显示服务器管理员密码修改弹窗 */
function showPasswordChangeModal() {
    const modal = document.getElementById('passwordChangeModal');
    if (!modal) return;
    const oldPasswordInput = document.getElementById('oldPasswordInput');
    const newPasswordInput = document.getElementById('newPasswordInput');
    oldPasswordInput.value = '';
    newPasswordInput.value = '';
    const oldHandler = oldPasswordInput._enterHandler;
    const newHandler = newPasswordInput._enterHandler;
    if (oldHandler) oldPasswordInput.removeEventListener('keypress', oldHandler);
    if (newHandler) newPasswordInput.removeEventListener('keypress', newHandler);
    const oldEnterHandler = (e) => { if (e.key === 'Enter') { e.preventDefault(); newPasswordInput.focus(); } };
    const newEnterHandler = (e) => { if (e.key === 'Enter') { e.preventDefault(); updateServerManagerPassword(); } };
    oldPasswordInput.addEventListener('keypress', oldEnterHandler);
    newPasswordInput.addEventListener('keypress', newEnterHandler);
    oldPasswordInput._enterHandler = oldEnterHandler;
    newPasswordInput._enterHandler = newEnterHandler;
    requestAnimationFrame(() => { modal.classList.add('show'); setTimeout(() => { oldPasswordInput.focus(); }, 50); });
}

/** 显示管理员密码修改弹窗 */
function showAdminPasswordChangeModal() {
    const modal = document.getElementById('adminPasswordChangeModal');
    if (!modal) return;
    const oldPasswordInput = document.getElementById('adminOldPasswordInput');
    const newPasswordInput = document.getElementById('adminNewPasswordInput');
    oldPasswordInput.value = '';
    newPasswordInput.value = '';
    const oldHandler = oldPasswordInput._enterHandler;
    const newHandler = newPasswordInput._enterHandler;
    if (oldHandler) oldPasswordInput.removeEventListener('keypress', oldHandler);
    if (newHandler) newPasswordInput.removeEventListener('keypress', newHandler);
    const oldEnterHandler = (e) => { if (e.key === 'Enter') { e.preventDefault(); newPasswordInput.focus(); } };
    const newEnterHandler = (e) => { if (e.key === 'Enter') { e.preventDefault(); updateAdminPassword(); } };
    oldPasswordInput.addEventListener('keypress', oldEnterHandler);
    newPasswordInput.addEventListener('keypress', newEnterHandler);
    oldPasswordInput._enterHandler = oldEnterHandler;
    newPasswordInput._enterHandler = newEnterHandler;
    requestAnimationFrame(() => { modal.classList.add('show'); setTimeout(() => { oldPasswordInput.focus(); }, 50); });
}

/** 关闭管理员密码修改弹窗 */
function closeAdminPasswordChangeModal() {
    const modal = document.getElementById('adminPasswordChangeModal');
    if (!modal) return;
    modal.classList.remove('show');
    setTimeout(() => { document.getElementById('adminOldPasswordInput').value = ''; document.getElementById('adminNewPasswordInput').value = ''; }, 300);
}

/** 关闭服务器管理员密码修改弹窗 */
function closePasswordChangeModal() {
    const modal = document.getElementById('passwordChangeModal');
    if (!modal) return;
    modal.classList.remove('show');
    setTimeout(() => { document.getElementById('oldPasswordInput').value = ''; document.getElementById('newPasswordInput').value = ''; }, 150);
}
//#endregion

//#region ID库功能模块
let idLibraryAccounts = [];

/** 从API加载ID库账户列表 */
async function loadIdLibraryFromAPI() {
    try {
        if (typeof API_BASE_URL === 'undefined') { return false; }
        const response = await fetch(`${API_BASE_URL}/id-library`, { method: 'GET', headers: { 'Content-Type': 'application/json' } });
        if (response.ok) {
            const data = await response.json();
            if (data.success && data.accounts) {
                idLibraryAccounts = data.accounts.map(acc => ({ ...acc, usageStatus: acc.usageStatus || 'new' }));
                return true;
            }
        }
    } catch (e) { }
    return false;
}

/** 从服务器同步ID库(API) */
async function syncIdLibraryFromServer() { return await loadIdLibraryFromAPI(); }

/** 上传ID库到服务器(API) */
async function syncIdLibraryToServer() {
    try {
        if (typeof API_BASE_URL === 'undefined') { return false; }
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 5000);
        const response = await fetch(`${API_BASE_URL}/id-library`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ accounts: idLibraryAccounts.map(acc => ({ appleId: acc.appleId, password: acc.password, status: acc.status || 'normal', usageStatus: acc.usageStatus || 'new' })) }), signal: controller.signal });
        clearTimeout(timeoutId);
        if (response.ok) { const data = await response.json(); if (data.success) { return true; } }
    } catch (e) { }
    return false;
}

/** 从服务器删除ID(API) */
async function deleteIdFromServer(appleId) {
    try {
        if (typeof API_BASE_URL === 'undefined') { return false; }
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 5000);
        const response = await fetch(`${API_BASE_URL}/id-library/${encodeURIComponent(appleId)}`, { method: 'DELETE', headers: { 'Content-Type': 'application/json' }, signal: controller.signal });
        clearTimeout(timeoutId);
        return response.ok;
    } catch (e) { if (e.name !== 'AbortError') { } return false; }
}

/** 更新ID使用状态到服务器(API) */
async function updateIdUsageStatusOnServer(appleId, usageStatus) {
    try {
        if (typeof API_BASE_URL === 'undefined') { return false; }
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 5000);
        const response = await fetch(`${API_BASE_URL}/id-library/${encodeURIComponent(appleId)}`, { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ usageStatus: usageStatus }), signal: controller.signal });
        clearTimeout(timeoutId);
        return response.ok;
    } catch (e) { if (e.name !== 'AbortError') { } return false; }
}

/** 显示ID库管理弹窗 */
async function showIdLibraryModal() {
    const modal = document.getElementById('idLibraryModal');
    if (!modal) return;
    await loadIdLibraryFromAPI();
    renderIdLibraryList();
    document.getElementById('idLibraryAppleId').value = '';
    document.getElementById('idLibraryPassword').value = '';
    modal.style.display = 'flex';
    setTimeout(() => { modal.classList.add('show'); document.getElementById('idLibraryAppleId').focus(); }, 10);
}

/** 关闭ID库管理弹窗 */
function closeIdLibraryModal() {
    const modal = document.getElementById('idLibraryModal');
    if (!modal) return;
    modal.classList.remove('show');
    setTimeout(() => { modal.style.display = 'none'; }, 150);
}

/** 切换ID库密码显示/隐藏 */
function toggleIdLibraryPassword() {
    const input = document.getElementById('idLibraryPassword');
    const btn = input.parentElement.querySelector('.password-toggle-btn');
    if (input.type === 'password') { input.type = 'text'; btn.textContent = '🙈'; } else { input.type = 'password'; btn.textContent = '👁'; }
}

/** 保存ID库账户(新增或更新) */
async function saveIdLibraryAccount() {
    const appleId = document.getElementById('idLibraryAppleId').value.trim();
    const password = document.getElementById('idLibraryPassword').value.trim();
    if (!appleId) { await customAlert('请输入Apple ID'); return; }
    if (!password) { await customAlert('请输入密码'); return; }
    const exists = idLibraryAccounts.find(acc => acc.appleId.toLowerCase() === appleId.toLowerCase());
    if (exists) { if (await customConfirm(`账号 ${appleId} 已存在，是否更新密码？`)) { exists.password = password; exists.updatedAt = new Date().toISOString(); } else { return; } }
    else { idLibraryAccounts.push({ appleId: appleId, password: password, status: 'normal', usageStatus: 'new', createdAt: new Date().toISOString(), updatedAt: new Date().toISOString() }); }
    renderIdLibraryList();
    try { await syncIdLibraryToServer(); } catch (e) { }
    document.getElementById('idLibraryAppleId').value = '';
    document.getElementById('idLibraryPassword').value = '';
    document.getElementById('idLibraryAppleId').focus();
}

/** 导入ID库账户(批量从文件) */
function importIdLibraryAccounts() {
    const input = document.createElement('input');
    input.type = 'file';
    input.accept = '.txt,.csv';
    input.onchange = async (e) => {
        const file = e.target.files[0];
        if (!file) return;
        try {
            const text = await file.text();
            const lines = text.split(/\r?\n/).filter(line => line.trim());
            let imported = 0;
            let skipped = 0;
            for (const line of lines) {
                const parts = line.split(/[,\t:\-]+/).map(p => p.trim());
                if (parts.length >= 2) {
                    const appleId = parts[0];
                    const password = parts[1];
                    if (appleId && password) {
                        const exists = idLibraryAccounts.find(acc => acc.appleId.toLowerCase() === appleId.toLowerCase());
                        if (!exists) {
                            idLibraryAccounts.push({ appleId: appleId, password: password, status: 'normal', usageStatus: 'new', createdAt: new Date().toISOString(), updatedAt: new Date().toISOString() });
                            imported++;
                        } else { skipped++; }
                    }
                }
            }
            renderIdLibraryList();
            try { await syncIdLibraryToServer(); } catch (e) { }
            await customAlert(`导入完成！\n新增: ${imported} 个\n跳过(已存在): ${skipped} 个`);
        } catch (err) { await customAlert('导入失败: ' + err.message); }
    };
    input.click();
}

/** 清空所有ID库账户 */
async function clearAllIdLibraryAccounts() {
    if (idLibraryAccounts.length === 0) { await customAlert('列表已为空'); return; }
    if (await customConfirm(`确定要清空所有 ${idLibraryAccounts.length} 个账号吗？\n此操作不可恢复！`)) {
        try { for (const account of idLibraryAccounts) { await deleteIdFromServer(account.appleId); } } catch (e) { }
        idLibraryAccounts = [];
        renderIdLibraryList();
    }
}

/** 删除单个ID库账户 */
async function deleteIdLibraryAccount(index) {
    if (index < 0 || index >= idLibraryAccounts.length) return;
    const account = idLibraryAccounts[index];
    if (await customConfirm(`确定要删除账号 ${account.appleId} 吗？`)) {
        idLibraryAccounts.splice(index, 1);
        renderIdLibraryList();
        try { await deleteIdFromServer(account.appleId); } catch (e) { }
    }
}

/** 填充ID库账户到输入框 */
function fillIdLibraryAccount(index) {
    if (index < 0 || index >= idLibraryAccounts.length) return;
    const account = idLibraryAccounts[index];
    document.getElementById('idLibraryAppleId').value = account.appleId;
    document.getElementById('idLibraryPassword').value = account.password;
    document.getElementById('idLibraryAppleId').focus();
}

/** 切换ID库账户状态(正常/异常) */
function toggleIdLibraryAccountStatus(index) {
    if (index < 0 || index >= idLibraryAccounts.length) return;
    const account = idLibraryAccounts[index];
    account.status = account.status === 'normal' ? 'error' : 'normal';
    account.updatedAt = new Date().toISOString();
    renderIdLibraryList();
    try { syncIdLibraryToServer(); } catch (e) { }
}

/** 切换ID库使用状态(NEW/USED) */
async function toggleIdLibraryUsageStatus(index) {
    if (index < 0 || index >= idLibraryAccounts.length) return;
    const account = idLibraryAccounts[index];
    const newStatus = account.usageStatus === 'new' ? 'used' : 'new';
    account.usageStatus = newStatus;
    account.updatedAt = new Date().toISOString();
    renderIdLibraryList();
    try { await updateIdUsageStatusOnServer(account.appleId, newStatus); } catch (e) { }
}

/** 掩码密码显示(保护隐私) */
function maskPassword(password) {
    if (!password) return '';
    if (password.length <= 4) return '****';
    return password.substring(0, 2) + '****' + password.substring(password.length - 2);
}

/** 渲染ID库账户列表 */
function renderIdLibraryList() {
    const listContainer = document.getElementById('idLibraryList');
    if (!listContainer) return;
    if (idLibraryAccounts.length === 0) {
        listContainer.innerHTML = `<div class="id-library-empty"><div class="empty-icon">📭</div><div class="empty-text">暂无账号</div><div class="empty-hint">点击"保存"添加账号，或"导入"批量导入</div></div>`;
    } else {
        listContainer.innerHTML = idLibraryAccounts.map((account, index) => `
            <div class="id-library-item ${account.status === 'error' ? 'error' : ''}">
                <div class="item-col col-index">${index + 1}</div>
                <div class="item-col col-account">${escapeHtml(account.appleId)}</div>
                <div class="item-col col-password">${maskPassword(account.password)}</div>
                <div class="item-col col-status"><span class="status-badge ${account.status || 'normal'}" onclick="toggleIdLibraryAccountStatus(${index})" style="cursor: pointer;" title="点击切换状态">${(account.status || 'normal') === 'normal' ? '正常' : '异常'}</span></div>
                <div class="item-col col-usage-status"><span class="usage-status-badge ${account.usageStatus || 'new'}" onclick="toggleIdLibraryUsageStatus(${index})" title="点击切换使用状态">${(account.usageStatus || 'new') === 'new' ? 'NEW' : 'USED'}</span></div>
                <div class="item-col col-actions"><button class="item-action-btn btn-fill" onclick="fillIdLibraryAccount(${index})" title="填充到输入框">填充</button><button class="item-action-btn btn-delete" onclick="deleteIdLibraryAccount(${index})" title="删除此账号">删除</button></div>
            </div>`).join('');
    }
    const normalCount = idLibraryAccounts.filter(acc => acc.status === 'normal').length;
    const errorCount = idLibraryAccounts.filter(acc => acc.status === 'error').length;
    document.getElementById('idLibraryTotal').textContent = idLibraryAccounts.length;
    document.getElementById('idLibraryNormal').textContent = normalCount;
    document.getElementById('idLibraryError').textContent = errorCount;
}

/** HTML转义(防XSS) */
function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}
//#endregion

//#region 超级管理员面板 - 充值功能
let saCurrentRechargeUserId = null;

/** 显示超级管理员充值面板 */
function showSuperAdminRechargePanel() {
    const serversSection = document.getElementById('superAdminServersSection');
    const detailSection = document.getElementById('superAdminDetailSection');
    const rechargeSection = document.getElementById('superAdminRechargeSection');
    if (serversSection) serversSection.style.display = 'none';
    if (detailSection) detailSection.style.display = 'none';
    if (rechargeSection) rechargeSection.style.display = 'block';
    const sidebarBtns = document.querySelectorAll('.super-admin-sidebar .sidebar-btn');
    sidebarBtns.forEach(btn => btn.classList.remove('active'));
    sidebarBtns.forEach(btn => { if (btn.textContent.includes('Recharge')) { btn.classList.add('active'); } });
}

/** 显示超级管理员服务器面板 */
function showSuperAdminServersPanel() {
    const serversSection = document.getElementById('superAdminServersSection');
    const detailSection = document.getElementById('superAdminDetailSection');
    const rechargeSection = document.getElementById('superAdminRechargeSection');
    if (serversSection) serversSection.style.display = 'block';
    if (detailSection) detailSection.style.display = 'none';
    if (rechargeSection) rechargeSection.style.display = 'none';
    const sidebarBtns = document.querySelectorAll('.super-admin-sidebar .sidebar-btn');
    sidebarBtns.forEach(btn => btn.classList.remove('active'));
    sidebarBtns.forEach(btn => { if (btn.textContent.includes('Servers')) { btn.classList.add('active'); } });
}

/** 验证充值用户并加载信息 */
async function saVerifyRechargeUser() {
    const userId = document.getElementById('saRechargeUserIdInput').value.trim();
    if (!userId) { saShowToast('请输入用户名', 'warning'); return; }
    try {
        const creditsResp = await fetch(`${API_BASE_URL}/user/${userId}/credits`);
        if (!creditsResp.ok) { if (creditsResp.status === 404) { saShowToast('用户不存在', 'error'); return; } throw new Error('获取用户信息失败'); }
        const creditsData = await creditsResp.json();
        if (!creditsData.success) { saShowToast('用户不存在', 'error'); return; }
        const userResp = await fetch(`${API_BASE_URL}/user/${userId}/statistics`);
        let userData = null;
        if (userResp.ok) { const data = await userResp.json(); if (data.success) { userData = data; } }
        saCurrentRechargeUserId = creditsData.user_id || userId;
        const credits = creditsData.credits || 0;
        const usage = userData?.usage || [];
        const rechargeRecords = usage.filter(item => item.action === 'recharge');
        const lastRecharge = rechargeRecords.length > 0 ? rechargeRecords[rechargeRecords.length - 1] : null;
        let totalSpent = 0;
        usage.forEach(item => { if (item.action !== 'recharge' && item.credits) { totalSpent += parseFloat(item.credits) || 0; } });
        let userIdDisplay = saCurrentRechargeUserId;
        if (userIdDisplay && userIdDisplay.startsWith('u_')) { userIdDisplay = userIdDisplay.substring(2); }
        const usernameDisplay = creditsData.username || userData?.username || userId || '-';
        document.getElementById('saRechargeInfoUserId').textContent = userIdDisplay;
        document.getElementById('saRechargeInfoUsername').textContent = usernameDisplay;
        document.getElementById('saRechargeInfoCredits').textContent = credits.toFixed(2);
        if (lastRecharge) { const lastRechargeTime = lastRecharge.ts ? new Date(lastRecharge.ts).toLocaleString('zh-CN', { year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit' }) : '-'; const lastRechargeAmount = parseFloat(lastRecharge.amount || 0).toFixed(2); document.getElementById('saRechargeInfoLastRecharge').textContent = `${lastRechargeTime} (+${lastRechargeAmount})`; }
        else { document.getElementById('saRechargeInfoLastRecharge').textContent = '无'; }
        document.getElementById('saRechargeInfoTotalSpent').textContent = totalSpent.toFixed(2);
        const createdDate = userData?.created ? new Date(userData.created).toLocaleString('zh-CN') : '-';
        document.getElementById('saRechargeInfoCreated').textContent = createdDate;
        document.getElementById('saRechargeUserInfoPanel').style.visibility = 'visible';
        document.getElementById('saRechargeUserInfoPanel').style.opacity = '1';
        const userInfoPanel = document.querySelector('.sa-recharge-user-info');
        if (userInfoPanel) { userInfoPanel.classList.add('show'); }
        const rechargeActionPanel = document.getElementById('saRechargeActionPanel');
        if (rechargeActionPanel) { rechargeActionPanel.style.visibility = 'visible'; rechargeActionPanel.style.opacity = '1'; }
        const userRechargeRecords = rechargeRecords.map(r => ({ ...r, user_id: saCurrentRechargeUserId, username: usernameDisplay }));
        saDisplayRechargeRecords(userRechargeRecords);
        document.getElementById('saRechargeRecordsList').style.display = 'block';
    } catch (error) { saShowToast('验证用户失败: ' + error.message, 'error'); }
}

/** 确认执行充值操作 */
async function saConfirmRecharge() {
    if (!saCurrentRechargeUserId) { saShowToast('请先验证用户', 'warning'); return; }
    const amount = parseFloat(document.getElementById('saRechargeAmountInput').value);
    if (!amount || amount === 0) { saShowToast('请输入有效的充值金额（支持负数）', 'warning'); return; }
    if (!await customConfirm(`确认给用户 ${saCurrentRechargeUserId} 充值 ${amount} 积分吗？`, 'dark-theme')) { return; }
    try {
        const response = await fetch(`${API_BASE_URL}/admin/users/${saCurrentRechargeUserId}/recharge`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ amount: amount }) });
        const data = await response.json();
        if (data.success) { const amountDisplay = amount >= 0 ? `+${amount.toFixed(2)}` : amount.toFixed(2); saShowToast(`充值成功！${amountDisplay} 积分，当前余额: ${data.credits.toFixed(2)}`, 'success'); document.getElementById('saRechargeAmountInput').value = ''; await saVerifyRechargeUser(); }
        else { saShowToast('充值失败: ' + (data.message || '未知错误'), 'error'); }
    } catch (error) { saShowToast('充值失败: ' + error.message, 'error'); }
}

/** 重置充值面板状态 */
function saResetRecharge() {
    saCurrentRechargeUserId = null;
    document.getElementById('saRechargeUserIdInput').value = '';
    document.getElementById('saRechargeAmountInput').value = '';
    document.getElementById('saRechargeUserInfoPanel').style.visibility = 'hidden';
    document.getElementById('saRechargeUserInfoPanel').style.opacity = '0';
    const userInfoPanel = document.querySelector('.sa-recharge-user-info');
    if (userInfoPanel) { userInfoPanel.classList.remove('show'); }
    const rechargeActionPanel = document.getElementById('saRechargeActionPanel');
    if (rechargeActionPanel) { rechargeActionPanel.style.visibility = 'hidden'; rechargeActionPanel.style.opacity = '0'; }
    document.getElementById('saRechargeRecordsList').style.display = 'none';
}

let saRechargeRecordsVisible = false;

/** 切换充值记录显示/隐藏 */
async function saToggleRechargeRecords() {
    const recordsList = document.getElementById('saRechargeRecordsList');
    if (!recordsList) return;
    if (saRechargeRecordsVisible) { recordsList.style.display = 'none'; saRechargeRecordsVisible = false; }
    else { recordsList.style.display = 'block'; recordsList.innerHTML = '<div class="log-line system">加载充值记录中...</div>'; try { const response = await fetch(`${API_BASE_URL}/admin/recharge-records`); if (response.ok) { const data = await response.json(); if (data.success && data.records && data.records.length > 0) { saDisplayRechargeRecords(data.records); } else { recordsList.innerHTML = '<div class="log-line system">暂无充值记录</div>'; } } else { recordsList.innerHTML = '<div class="log-line error">加载失败，请刷新重试</div>'; } } catch (error) { recordsList.innerHTML = '<div class="log-line error">加载失败，请刷新重试</div>'; } saRechargeRecordsVisible = true; }
}

/** 渲染充值记录列表HTML */
function saDisplayRechargeRecords(records) {
    const container = document.getElementById('saRechargeRecordsList');
    if (!container) return;
    if (!records || records.length === 0) { container.innerHTML = '<div class="log-line system">暂无充值记录</div>'; return; }
    const sortedRecords = records.sort((a, b) => { return new Date(b.ts || 0).getTime() - new Date(a.ts || 0).getTime(); });
    let html = '';
    sortedRecords.forEach((record, index) => {
        const time = record.ts ? new Date(record.ts).toLocaleString('zh-CN', { month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit' }) : '-';
        const amount = parseFloat(record.amount || 0);
        const amountDisplay = amount >= 0 ? `+${amount.toFixed(2)}` : amount.toFixed(2);
        const amountColor = amount >= 0 ? '#00ff88' : '#ff4757';
        const username = record.username || record.user_id || '-';
        html += `<div class="log-line" style="display: flex; justify-content: space-between; padding: 4px 0; border-bottom: 1px solid #333;">`;
        html += `<span style="color: #888;">${index + 1}.</span>`;
        html += `<span style="color: #4facfe; flex: 1; margin-left: 10px;">${username}</span>`;
        html += `<span style="color: #888; margin-right: 15px;">${time}</span>`;
        html += `<span style="color: ${amountColor}; font-weight: bold; min-width: 80px; text-align: right;">${amountDisplay}</span>`;
        html += `</div>`;
    });
    container.innerHTML = html;
}

/** 切换超级管理员侧边栏Tab */
/** 切换超级管理员侧边栏Tab */
function switchSuperAdminTab(tabName) {
    const sections = ['superAdminServersSection', 'superAdminUserSection', 'superAdminRechargeSection', 'superAdminRatesSection'];
    sections.forEach(id => { const el = document.getElementById(id); if (el) el.style.display = 'none'; });
    if (tabName === 'servers') { showSuperAdminServersPanel(); }
    else if (tabName === 'users') { const el = document.getElementById('superAdminUserSection'); if (el) el.style.display = 'block'; _updateSaSidebarActive('users'); }
    else if (tabName === 'recharge') { showSuperAdminRechargePanel(); }
    else if (tabName === 'rates') { const el = document.getElementById('superAdminRatesSection'); if (el) el.style.display = 'block'; _updateSaSidebarActive('rates'); }
    else if (tabName === 'logs') { _updateSaSidebarActive('logs'); }
    else if (tabName === 'settings') { _updateSaSidebarActive('settings'); }
}

/** 更新超级管理员侧边栏激活状态 */
function _updateSaSidebarActive(tabName) {
    const sidebarBtns = document.querySelectorAll('.super-admin-sidebar .sidebar-btn');
    sidebarBtns.forEach(btn => { btn.classList.remove('active'); const onclick = btn.getAttribute('onclick'); if (onclick && onclick.includes(`'${tabName}'`)) { btn.classList.add('active'); } });
}
//#endregion

//#region 密码修改
/** 更新服务器管理员密码(API) */
async function updateServerManagerPassword() {
    const oldPassword = document.getElementById('oldPasswordInput').value.trim();
    const newPassword = document.getElementById('newPasswordInput').value.trim();
    if (!oldPassword) { await customAlert('请输入旧密码'); document.getElementById('oldPasswordInput').focus(); return; }
    if (!newPassword) { await customAlert('请输入新密码'); document.getElementById('newPasswordInput').focus(); return; }
    try {
        const response = await fetch(`${API_BASE_URL}/server-manager/password`, { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ oldPassword: oldPassword, password: newPassword }) });
        const data = await response.json();
        if (data.success) { setTimeout(async () => { await customAlert('修改成功'); closePasswordChangeModal(); }, 100); }
        else { await customAlert(data.message || '更新失败，请检查旧密码是否正确'); document.getElementById('oldPasswordInput').focus(); }
    } catch (error) { await customAlert('网络错误，请检查API服务器连接'); }
}

/** 更新管理员账户密码(API) */
async function updateAdminPassword() {
    const oldPassword = document.getElementById('adminOldPasswordInput').value.trim();
    const newPassword = document.getElementById('adminNewPasswordInput').value.trim();
    if (!oldPassword) { await customAlert('请输入旧密码'); document.getElementById('adminOldPasswordInput').focus(); return; }
    if (!newPassword) { await customAlert('请输入新密码'); document.getElementById('adminNewPasswordInput').focus(); return; }
    let managerId = currentManagerId;
    if (!managerId) { managerId = StorageManager.session.getCurrentManagerId(); }
    if (!managerId) { await customAlert('未找到当前管理员ID，请重新登录'); return; }
    try {
        const loginResponse = await fetch(`${API_BASE_URL}/admin/login`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ admin_id: managerId, password: oldPassword }) });
        const loginData = await loginResponse.json();
        if (!loginData.success) { await customAlert('旧密码错误'); document.getElementById('adminOldPasswordInput').focus(); return; }
        const token = StorageManager.session.getServerManagerToken();
        const headers = { 'Content-Type': 'application/json' };
        if (token) headers['Authorization'] = `Bearer ${token}`;
        const response = await fetch(`${API_BASE_URL}/admin/account/${managerId}`, { method: 'PUT', headers: headers, body: JSON.stringify({ password: newPassword }) });
        const data = await response.json();
        if (data.success) { setTimeout(async () => { await customAlert('修改成功'); closeAdminPasswordChangeModal(); }, 100); }
        else { await customAlert(data.message || '更新失败'); document.getElementById('adminOldPasswordInput').focus(); }
    } catch (error) { await customAlert('网络错误，请检查API服务器连接'); }
}

/** 编辑管理员账户密码 */
async function editAdminAccount(adminId) {
    const account = adminAccounts.find(a => a.id === adminId);
    if (!account) return;
    const newPassword = await customPrompt('请输入新密码:', account.password);
    if (newPassword && newPassword.trim()) { account.password = newPassword.trim(); updateAdminAccountDisplay(); }
}

/** 删除管理员账户(API) */
async function deleteAdminAccount(adminId) {
    const confirmed = await customConfirm('确定要删除该管理员账户吗？');
    if (confirmed) {
        try {
            const token = StorageManager.session.getServerManagerToken();
            const headers = { 'Content-Type': 'application/json' };
            if (token) headers['Authorization'] = `Bearer ${token}`;
            const response = await fetch(`${API_BASE_URL}/admin/account/${adminId}`, { method: 'DELETE', headers: headers });
            if (response.ok) {
                adminAccounts = adminAccounts.filter(a => a.id !== adminId);
                StorageManager.admin.addDeletedAdminId(adminId);
                updateAdminAccountDisplay();
            } else { const errorData = await response.json(); await customAlert(errorData.message || '删除管理员账号失败'); }
        } catch (error) { await customAlert('无法连接到API服务器: ' + error.message); }
    }
}

/** 更新管理员账户显示列表 */
function updateAdminAccountDisplay() {
    const container = document.getElementById('adminAccountList');
    const fragment = document.createDocumentFragment();
    container.innerHTML = '';
    adminAccounts.forEach(account => {
        const item = document.createElement('div');
        item.className = 'admin-account-item';
        const serverCount = (account.selectedServers && account.selectedServers.length) || 0;
        item.innerHTML = `<span class="admin-account-name"><span class="admin-account-badge ${serverCount > 0 ? 'flash' : ''}">${serverCount}</span>${account.id}</span><div class="admin-account-actions"><button class="admin-account-action-btn manage" onclick="manageAdminAccount('${account.id}')">管理</button></div>`;
        fragment.appendChild(item);
    });
    container.appendChild(fragment);
}
//#endregion

//#region 管理员账户管理
let tempSelectedServers = [];

/** 管理管理员账户(分配服务器) */
function manageAdminAccount(adminId) {
    const account = adminAccounts.find(a => a.id === adminId);
    if (!account) return;
    tempSelectedServers = [...(account.selectedServers || [])];
    const allServerObjs = [...serverData.connected];
    const allServers = allServerObjs.map(s => s.name || s.server_name || s.server_id);
    const assignedServersMap = new Map();
    adminAccounts.forEach(acc => { if (acc.id !== adminId && Array.isArray(acc.selectedServers)) { acc.selectedServers.forEach(s => assignedServersMap.set(String(s).trim(), acc.id)); } });
    serverData.connected.forEach(server => { if (server.assigned_user_id) { assignedServersMap.set(String(server.name || server.server_name || server.server_id).trim(), 'USER'); } });
    const panel = document.getElementById('customModalPanel');
    panel.classList.add('admin-manage-modal');
    const content = document.getElementById('customModalContent');
    content.className = 'admin-manage-content';
    const titleEl = document.getElementById('customModalTitle');
    const messageEl = document.getElementById('customModalMessage');
    const buttonsEl = document.getElementById('customModalButtons');
    const inputEl = document.getElementById('customModalInput');
    const performanceDisplay = account.totalPerformance ? account.totalPerformance : '0.00';
    titleEl.style.display = 'flex';
    titleEl.style.alignItems = 'center';
    titleEl.style.width = '100%';
    titleEl.innerHTML = `<span>管理员: <span class="admin-id-badge">${account.id}</span></span><span class="admin-performance-badge">业绩: ${performanceDisplay}</span>`;
    messageEl.innerHTML = `<div style="display: flex; justify-content: space-between; align-items: center; padding: 10px 15px; background: rgba(0,0,0,0.05); border-radius: 8px; margin-bottom: 10px;"><div style="display: flex; gap: 25px; align-items: center;"><span><strong style="color:#666;">ID:</strong> ${account.id}</span><span><strong style="color:#666;">密码:</strong> <span style="font-family:monospace;">${account.password || '***'}</span></span></div><div style="display: flex; gap: 10px;"><button class="admin-account-action-btn edit" onclick="editAdminPasswordInModal('${adminId}')" style="padding:5px 12px;font-size:12px;">修改密码</button><button class="admin-account-action-btn delete" onclick="deleteAdminInModal('${adminId}')" style="padding:5px 12px;font-size:12px;">删除账号</button></div></div><div style="display: flex; gap: 25px; align-items: center; padding: 8px 15px; margin-bottom: 15px; color: #666; font-size: 13px;"><span><strong>推广用户:</strong> ${account.userCount || 0}</span><span><strong>费率:</strong> ${account.rate || '-'}</span><span><strong>上次访问:</strong> ${account.lastAccess || '-'}</span></div><div style="font-size: 14px; font-weight: bold; color: #333; margin-bottom: 10px; padding-left: 15px;">私享服务器授权</div><div class="server-buttons-grid" id="adminManageServersGrid" style="margin: 0 15px; width: calc(100% - 30px);">${allServers.length > 0 ? (() => { const availableServers = []; const assignedToThisAdmin = []; const assignedToOthers = []; allServers.forEach(server => { const serverStr = String(server).trim(); const assignedOwner = assignedServersMap.get(serverStr); const isSelected = tempSelectedServers.some(selected => String(selected).trim() === serverStr); if (isSelected) { assignedToThisAdmin.push(server); } else if (assignedOwner) { assignedToOthers.push({ server, owner: assignedOwner }); } else { availableServers.push(server); } }); const generateServerBtn = (server, assignedOwner = null) => { const serverStr = String(server).trim(); const isSelected = tempSelectedServers.some(selected => String(selected).trim() === serverStr); const escapedServer = serverStr.replace(/"/g, '&quot;').replace(/'/g, '&#39;').replace(/\\/g, '\\\\'); const portMatch = (allServerObjs.find(s => (s.name || s.server_name || s.server_id) === serverStr)?.url || '').match(/:(\d+)/); const port = portMatch ? portMatch[1] : (allServerObjs.find(s => (s.name || s.server_name || s.server_id) === serverStr)?.port || serverStr.match(/\d+/)?.[0] || '?'); const statusText = isSelected ? '状态: 已选中' : '状态: 已连接'; const botHTML = SERVER_BOT_HTML; if (assignedOwner) { return '<button class="server-button connected private" disabled style="cursor: not-allowed; pointer-events: auto;" data-server-name="' + escapedServer + '">' + botHTML + '<div class="server-button-name" style="position: absolute; bottom: -20px; left: 50%; transform: translateX(-50%); font-size: 11px; color: #e1bee7; white-space: nowrap; pointer-events: none; z-index: 100;">' + port + '</div><div class="server-tooltip"><div style="font-weight: bold; margin-bottom: 4px;">' + escapedServer + '</div><div style="font-size: 11px; color: #ffeb3b; margin-top: 4px;" class="status-text">私享服务器</div><div style="font-size: 11px; color: #fff; margin-top: 2px;">已分配: ' + assignedOwner + '</div></div></button>'; } return '<button class="server-button connected ' + (isSelected ? 'selected' : '') + '" data-server-name="' + escapedServer + '" onclick="toggleTempServerSelection(\'' + adminId + '\', \'' + escapedServer + '\', this)">' + botHTML + '<div class="server-button-name" style="position: absolute; bottom: -20px; left: 50%; transform: translateX(-50%); font-size: 11px; color: #2d3436; white-space: nowrap; pointer-events: none; z-index: 100;">' + port + '</div><div class="server-tooltip"><div style="font-weight: bold; margin-bottom: 4px;">' + escapedServer + '</div><div style="font-size: 11px; color: ' + (isSelected ? '#ffd700' : '#00ff88') + '; margin-top: 4px;" class="status-text">' + statusText + '</div></div></button>'; }; let html = ''; availableServers.forEach(server => { html += generateServerBtn(server); }); assignedToThisAdmin.forEach(server => { html += generateServerBtn(server); }); assignedToOthers.forEach(item => { html += generateServerBtn(item.server, item.owner); }); return html; })() : '<div style="color: #999; padding: 20px; text-align: center; width: 100%;">暂无可用服务器</div>'}</div>`;
    inputEl.style.display = 'none';
    buttonsEl.innerHTML = `<button class="admin-manage-footer-btn cancel" onclick="closeCustomModal()">取消</button><button class="admin-manage-footer-btn reset" onclick="resetAdminSelectionTemp('${adminId}')">重置</button><button class="admin-manage-footer-btn select-all" onclick="selectAllServersTemp('${adminId}')">全选</button><button class="admin-manage-footer-btn confirm" onclick="confirmAdminManage('${adminId}')">确定保存</button>`;
    const modal = document.getElementById('customModal');
    requestAnimationFrame(() => { modal.classList.add('show'); });
}

function toggleTempServerSelection(adminId, serverName, button) {
    const index = tempSelectedServers.indexOf(serverName);
    if (index > -1) { tempSelectedServers.splice(index, 1); button.classList.remove('selected'); }
    else { tempSelectedServers.push(serverName); button.classList.add('selected'); }
    const statusText = button.querySelector('.status-text');
    if (statusText) { statusText.textContent = button.classList.contains('selected') ? '状态: 已选中' : '状态: 已连接'; statusText.style.color = button.classList.contains('selected') ? '#ffd700' : '#00ff88'; }
}

/** 重置管理员临时服务器选择 */
function resetAdminSelectionTemp(adminId) {
    tempSelectedServers = [];
    const grid = document.getElementById('adminManageServersGrid');
    const buttons = grid.querySelectorAll('.server-button');
    buttons.forEach(btn => { btn.classList.remove('selected'); const statusText = btn.querySelector('.status-text'); if (statusText) { statusText.textContent = '状态: 已连接'; statusText.style.color = '#00ff88'; } });
}

/** 全选可用服务器(临时) */
function selectAllServersTemp(adminId) {
    const allConnectedServers = serverData.connected.map(s => s.name || s.server_name || s.server_id || String(s));
    const availableServers = allConnectedServers.filter(serverName => {
        const isAssignedToOther = adminAccounts.some(acc => { return acc.id !== adminId && Array.isArray(acc.selectedServers) && acc.selectedServers.some(s => String(s).trim() === String(serverName).trim()); });
        const isAssignedToUser = serverData.connected.some(server => { return server.assigned_user_id && String(server.name || server.server_name || server.server_id).trim() === String(serverName).trim(); });
        return !isAssignedToOther && !isAssignedToUser;
    });
    tempSelectedServers = [...availableServers];
    const grid = document.getElementById('adminManageServersGrid');
    if (grid) {
        const buttons = grid.querySelectorAll('.server-button');
        buttons.forEach(btn => { if (btn.classList.contains('private')) return; let serverName = btn.dataset.serverName; if (serverName && availableServers.some(s => String(s).trim() === String(serverName).trim())) { btn.classList.add('selected'); const statusText = btn.querySelector('.status-text'); if (statusText) { statusText.textContent = '状态: 已选中'; statusText.style.color = '#ffd700'; } } else if (serverName) { } });
    }
}

/** 确认管理员管理配置(保存服务器分配) */
async function confirmAdminManage(adminId) {
    const account = adminAccounts.find(a => a.id === adminId);
    if (!account) return;
    const grid = document.getElementById('adminManageServersGrid');
    if (grid) {
        const selectedButtons = grid.querySelectorAll('.server-button.selected');
        const selectedServers = Array.from(selectedButtons).map(btn => { if (btn.dataset.serverName) { return btn.dataset.serverName.trim(); } const tooltip = btn.querySelector('.server-tooltip'); if (tooltip) { const nameDiv = tooltip.querySelector('div[style*="font-weight: bold"]'); if (nameDiv) return nameDiv.textContent.trim(); } return (btn.textContent || '').trim(); }).filter(Boolean);
        const conflicts = [];
        selectedServers.forEach(serverName => { const isAssignedToOther = adminAccounts.some(acc => { return acc.id !== adminId && Array.isArray(acc.selectedServers) && acc.selectedServers.some(s => String(s).trim() === String(serverName).trim()); }); if (isAssignedToOther) { conflicts.push(serverName); } });
        if (conflicts.length > 0) { await customAlert(`以下服务器已被其他管理员分配，无法重复分配：\n${conflicts.join(', ')}`); return; }
        account.selectedServers = selectedServers;
    }
    try {
        const token = StorageManager.session.getServerManagerToken();
        const headers = { 'Content-Type': 'application/json' };
        if (token) headers['Authorization'] = `Bearer ${token}`;
        const response = await fetch(`${API_BASE_URL}/admin/account/${adminId}`, { method: 'PUT', headers: headers, body: JSON.stringify({ selected_servers: account.selectedServers || [] }) });
        if (!response.ok) { const err = await response.json().catch(() => ({})); }
    } catch (error) { }
    closeCustomModal();
    updateAdminAccountDisplay();
    setTimeout(async () => { await customAlert('管理员配置已保存'); }, 300);
}

/** 在弹窗中编辑管理员密码 */
async function editAdminPasswordInModal(adminId) {
    const account = adminAccounts.find(a => a.id === adminId);
    if (!account) return;
    const newPassword = await customPrompt('请输入新密码:');
    if (newPassword && newPassword.trim()) { setTimeout(() => { manageAdminAccount(adminId); }, 350); }
}

async function deleteAdminInModal(adminId) {
    const confirmed = await customConfirm('确定要删除该管理员账户吗？');
    if (!confirmed) { return; }
    try {
        const token = StorageManager.session.getServerManagerToken();
        const headers = { 'Content-Type': 'application/json' };
        if (token) headers['Authorization'] = `Bearer ${token}`;
        const response = await fetch(`${API_BASE_URL}/admin/account/${adminId}`, { method: 'DELETE', headers: headers });
        if (!response.ok) { const errorData = await response.json().catch(() => ({})); await customAlert(`删除失败：${errorData.message || response.statusText || '未知错误'}`); return; }
        adminAccounts = adminAccounts.filter(a => a.id !== adminId);
        closeCustomModal();
        updateAdminAccountDisplay();
        await customAlert('管理员账号已删除');
    } catch (error) { await customAlert(`无法连接到API服务器: ${error.message}`); }
}
//#endregion

//#region 密码修改选择面板
function showSelectPanelModal() {
    const m = document.getElementById('selectPanelModal');
    if (!m) return;
    m.classList.add('show');
}

function closeSelectPanelModal() {
    const m = document.getElementById('selectPanelModal');
    if (!m) return;
    m.classList.remove('show');
}

function onSelectServerManagerPanel() {
    closeSelectPanelModal();
    setTimeout(() => showPasswordChangeModal(), 150);
}

function onSelectSuperAdminPanel() {
    closeSelectPanelModal();
    setTimeout(() => showSuperAdminPasswordChangeModal(), 150);
}
//#endregion

//#region 超级管理员密码修改
function showSuperAdminPasswordChangeModal() {
    const m = document.getElementById('superAdminPasswordChangeModal');
    if (!m) return;
    const o = document.getElementById('superAdminOldPasswordInput');
    const n = document.getElementById('superAdminNewPasswordInput');
    o.value = '';
    n.value = '';
    m.classList.add('show');
    setTimeout(() => o.focus(), 50);
}

function closeSuperAdminPasswordChangeModal() {
    const m = document.getElementById('superAdminPasswordChangeModal');
    if (!m) return;
    m.classList.remove('show');
    setTimeout(() => { document.getElementById('superAdminOldPasswordInput').value = ''; document.getElementById('superAdminNewPasswordInput').value = ''; }, 150);
}

async function updateSuperAdminPassword() {
    const o = document.getElementById('superAdminOldPasswordInput').value;
    const n = document.getElementById('superAdminNewPasswordInput').value;
    if (!o || !n) { await customAlert('请填写完整信息'); return; }
    try {
        const r = await fetch(API_BASE_URL + '/server-manager/password', { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ oldPassword: o, password: n }) });
        const d = await r.json();
        if (d.success) { await customAlert('密码修改成功'); closeSuperAdminPasswordChangeModal(); }
        else { await customAlert(d.message || '密码修改失败'); }
    } catch (e) { await customAlert('网络错误，请稍后重试'); }
}
//#endregion
