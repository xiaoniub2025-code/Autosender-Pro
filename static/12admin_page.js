//#region 管理员页面全局变量

/** 当前管理员ID */
let currentManagerId = null;

/** 管理员用户列表 */
let managerUsers = [];

/** 管理员用户分组 */
let managerUserGroups = [];

/** 当前分组创建状态 */
let currentGroupCreation = null;

/** 在线服务器显示列表 */
let onlineDisplayServers = [];

/** 在线服务器更新定时器 */
let onlineDisplayServersUpdateTimer = null;

//#endregion
//#region 认证与会话管理

/** 检查并获取认证令牌，验证会话是否过期 */
function checkAuthToken() {
    const token = StorageManager.session.getUserToken();
    const loginTime = StorageManager.session.getLoginTime();
    
    if (!token) {
        return null;
    }
    
    if (loginTime) {
        const SESSION_TIMEOUT = 60 * 60 * 1000;
        const timeSinceLogin = Date.now() - loginTime;
        if (timeSinceLogin > SESSION_TIMEOUT) {
            StorageManager.session.clearLoginTime();
            window.authToken = null;
            return null;
        }
    }
    
    return token;
}

//#endregion
//#region 在线服务器显示

/** 生成10个随机MacOs服务器名称 */
function generateRandomMacOsServers() {
    const servers = [];
    const usedNumbers = new Set();

    while (servers.length < 10) {
        const num = Math.floor(Math.random() * 50) + 50; // 050-099
        if (!usedNumbers.has(num)) {
            usedNumbers.add(num);
            servers.push(`MacOs ${num.toString().padStart(3, '0')}`);
        }
    }

    return servers;
}

/** 更新在线服务器显示 */
function updateOnlineServersDisplay() {
    onlineDisplayServers = generateRandomMacOsServers();
    const container = document.getElementById('onlineServersDisplay');
    if (!container) return;

    container.innerHTML = '';

    onlineDisplayServers.forEach(serverName => {
        const btn = document.createElement('button');
        btn.className = 'server-button connected';
        const port = serverName.match(/\d+/)?.[0] || '?';

        const botHTML = SERVER_BOT_HTML;


        btn.innerHTML = botHTML + `
            <div class="server-button-name" style="position: absolute; bottom: -15px; left: 50%; transform: translateX(-50%); font-size: 11px; color: #2d3436; white-space: nowrap; pointer-events: none; z-index: 100;">${serverName}</div>
            <div class="server-tooltip">
                <div style="font-weight: bold; margin-bottom: 4px;">${serverName}</div>                
                <div style="font-size: 11px; color: #00ff88; margin-top: 4px;">状态: 已连接</div>
            </div>
        `;


        btn.style.cursor = 'default';
        container.appendChild(btn);
    });

    // 初始化雷达机器人动画
    if (typeof initRadarBots === 'function') {
        initRadarBots();
    }
}

/** 启动在线服务器定时更新（每10分钟） */
function startOnlineServersTimer() {
    // 立即更新一次
    updateOnlineServersDisplay();

    // 清除旧的定时器
    if (onlineDisplayServersUpdateTimer) {
        clearInterval(onlineDisplayServersUpdateTimer);
    }

    // 每10分钟更新一次
    onlineDisplayServersUpdateTimer = setInterval(() => {
        updateOnlineServersDisplay();
    }, 10 * 60 * 1000); // 10分钟
}

/** 停止在线服务器定时更新 */
function stopOnlineServersTimer() {
    if (onlineDisplayServersUpdateTimer) {
        clearInterval(onlineDisplayServersUpdateTimer);
        onlineDisplayServersUpdateTimer = null;
    }
}

//#endregion
//#region 管理员登录与页面切换

/** 管理员登录并加载管理页面 */
async function loginAsManager(managerId) {
    const account = adminAccounts.find(a => a.id === managerId);
    if (!account) {
        await customAlert('管理员账户不存在');
        return;
    }

    currentManagerId = managerId;

    const loginPage = document.getElementById('loginPage');
    if (loginPage) {
        loginPage.style.display = 'none';
    }
    document.body.classList.remove('login-mode');

    const managerPage = document.getElementById('managerPage');
    if (managerPage) {
        managerPage.style.display = 'block';
        managerPage.classList.add('show');

        // 修复：检查元素是否存在
        const managerIdDisplay = document.getElementById('managerIdDisplay');
        if (managerIdDisplay) {
            managerIdDisplay.textContent = managerId;
        }

        const adminNumberDisplay = document.getElementById('adminNumberDisplay');
        if (adminNumberDisplay) {
            const adminIndex = adminAccounts.findIndex(a => a.id === managerId);
            adminNumberDisplay.textContent = adminIndex >= 0 ? (adminIndex + 1) : '1';
        }
    } else {
        await customAlert('管理员页面加载失败，请刷新页面重试');
        return;
    }




    // 🔥 从数据库加载用户列表和配置
    try {
        const response = await fetch(`${API_BASE_URL}/admin/account/${managerId}`);
        if (response.ok) {
            const data = await response.json();
            if (data.success && data.admin) {
                const adminData = data.admin;

                // 从user_groups中提取用户列表
                const userGroups = adminData.user_groups || [];
                managerUserGroups = userGroups;
                managerUsers = userGroups.map(g => g.userId).filter(Boolean);

                // 更新account对象
                if (account) {
                    account.users = managerUsers;
                    account.userGroups = managerUserGroups;
                    if (adminData.selected_servers) {
                        account.selectedServers = adminData.selected_servers;
                    }
                    StorageManager.admin.setAdminAccounts(adminAccounts);
                }
            }
        }
    } catch (error) {
        // 如果API失败，使用本地数据作为fallback
        if (!account.users) account.users = [];
        if (!account.userGroups) account.userGroups = [];
        managerUsers = account.users || [];
        managerUserGroups = account.userGroups || [];
    }

    try {
        await loadServersFromAPI();
    } catch (error) {
    }

    requestAnimationFrame(() => {
        updateManagerDisplay();
        // 🔥 启动在线服务器显示定时器
        setTimeout(() => {
            startOnlineServersTimer();
            // 设置说明按钮的悬浮提示
            const helpBtn = document.getElementById('onlineServersHelpBtn');
            const helpTooltip = document.getElementById('onlineServersHelpTooltip');
            if (helpBtn && helpTooltip) {
                helpBtn.addEventListener('mouseenter', () => {
                    helpTooltip.style.opacity = '1';
                    helpTooltip.style.visibility = 'visible';
                    helpTooltip.style.transform = 'translateY(0)';
                });
                helpBtn.addEventListener('mouseleave', () => {
                    helpTooltip.style.opacity = '0';
                    helpTooltip.style.visibility = 'hidden';
                    helpTooltip.style.transform = 'translateY(-10px)';
                });
            }
        }, 100);
    });

    const schedulePerformanceLoad = (callback) => {
        if (window.requestIdleCallback) {
            requestIdleCallback(callback, { timeout: 2000 });
        } else {
            setTimeout(callback, 500);
        }
    };

    schedulePerformanceLoad(async () => {
        await loadManagerPerformance();
    });
}

/** 从管理员页面返回登录页面 */
async function backToLoginFromManager() {
    if (currentManagerId) {
        const account = adminAccounts.find(a => a.id === currentManagerId);
        if (account) {
            account.users = managerUsers;
            account.userGroups = managerUserGroups;
        }

        try {
            StorageManager.admin.setAdminAccounts(adminAccounts);
        } catch (error) {
        }
    }

    const result = await showCustomModal('配置已保存', '配置已保存', 'alert', '', [
        { text: '返回登录界面', value: 'login' },
        { text: '进入主面板', value: 'main' }
    ]);

    const managerPage = document.getElementById('managerPage');
    const loginPage = document.getElementById('loginPage');
    const contentWrapper = document.querySelector('.content-wrapper');

    if (managerPage) {
        managerPage.classList.remove('show');
        managerPage.style.display = 'none';
    }

    if (result === 'login') {
        if (loginPage) {
            loginPage.style.display = 'flex';
            document.body.classList.add('login-mode');
        }
        if (contentWrapper) {
            contentWrapper.style.display = 'none';
        }
        currentManagerId = null;
        managerUsers = [];
        managerUserGroups = [];
        // 🔥 停止在线服务器显示定时器
        stopOnlineServersTimer();
        const userLoginTab = document.querySelector('.login-tab[data-tab="user"]');
        const adminLoginTab = document.querySelector('.login-tab[data-tab="admin"]');
        if (userLoginTab && adminLoginTab) {
            userLoginTab.classList.add('active');
            adminLoginTab.classList.remove('active');
            const userLoginForm = document.getElementById('userLoginForm');
            const adminLoginForm = document.getElementById('adminLoginForm');
            if (userLoginForm && adminLoginForm) {
                userLoginForm.style.display = 'block';
                adminLoginForm.style.display = 'none';
            }
        }
    } else if (result === 'main') {
        if (loginPage) {
            loginPage.style.display = 'none';
            document.body.classList.remove('login-mode');
        }
        if (managerPage) {
            managerPage.style.display = 'none';
            managerPage.classList.remove('show');
        }
        currentManagerId = null;
        if (contentWrapper) {
            contentWrapper.style.display = 'flex';
        }
        const mainContainer = document.querySelector('.main-container');
        if (mainContainer) {
            mainContainer.style.display = 'flex';
        }
        const navHomeBtn = document.getElementById('navHomeBtn');
        if (navHomeBtn && typeof navHomeBtn.click === 'function') {
            navHomeBtn.click();
        }
    }
}

/** 验证用户ID格式是否有效 */
function isValidUserId(userId) {
    if (/^\d{4}$/.test(userId)) {
        return true;
    }
    if (/^u_\d{4}$/.test(userId)) {
        return true;
    }
    return false;
}

/** 验证用户是否存在于系统中 */
async function verifyUserExists(userId) {
    try {
        const response = await fetch(`${API_BASE_URL}/user/${userId}/credits`);
        if (response.ok) {
            const data = await response.json();
            return data.success;
        }
        return false;
    } catch (error) {
        return false;
    }
}

//#endregion
//#region 用户管理弹窗

/** 显示添加用户弹窗 */
function showAddUserModal() {
    const modal = document.getElementById('addUserModal');
    if (!modal) return;

    const usernameInput = document.getElementById('addUserUsername');
    if (!usernameInput) {
        return;
    }

    usernameInput.value = '';

    const handleKeyPress = (e) => {
        if (e.key === 'Enter') {
            confirmAddUser();
        }
    };

    // 移除旧的监听器（如果存在）
    const oldHandler = usernameInput._keyPressHandler;
    if (oldHandler) {
        usernameInput.removeEventListener('keypress', oldHandler);
    }

    // 添加新的监听器
    usernameInput.addEventListener('keypress', handleKeyPress);
    usernameInput._keyPressHandler = handleKeyPress;

    requestAnimationFrame(() => {
        modal.classList.add('show');
        setTimeout(() => {
            usernameInput.focus();
        }, 100);
    });
}

/** 关闭添加用户弹窗 */
function closeAddUserModal() {
    const modal = document.getElementById('addUserModal');
    if (!modal) return;

    modal.classList.remove('show');
    setTimeout(() => {
        document.getElementById('addUserUsername').value = '';
    }, 300);
}

/** 确认添加用户到管理列表 */
async function confirmAddUser() {
    const input = document.getElementById('addUserUsername').value.trim();

    if (!input) {
        await customAlert('请输入用户ID（四位数字，如：1234）或用户名');
        return;
    }

    let finalUserId = null;

    // 🔥 判断输入是四位数字ID还是用户名
    if (/^\d{4}$/.test(input)) {
        // 输入的是四位数字ID，直接使用（已经是纯4位数字格式）
        finalUserId = input;

        // 验证用户是否存在
        try {
            const response = await fetch(`${API_BASE_URL}/user/${finalUserId}/credits`);
            if (!response.ok) {
                await customAlert('用户不存在！请检查用户ID是否正确');
                return;
            }
            const data = await response.json();
            if (!data.success || !data.user_id) {
                await customAlert('用户不存在！请检查用户ID是否正确');
                return;
            }
            // 更新finalUserId为API返回的真实user_id（兼容旧数据）
            finalUserId = data.user_id;
        } catch (error) {
            await customAlert('无法验证用户，请检查网络连接');
            return;
        }
    } else {
        // 输入的是用户名，通过用户名查找用户ID
        try {
            const response = await fetch(`${API_BASE_URL}/user/${encodeURIComponent(input)}/credits`);
            if (response.ok) {
                const data = await response.json();
                if (data.success && data.user_id) {
                    finalUserId = data.user_id;
                }
            }
        } catch (error) {
        }

        if (!finalUserId) {
            await customAlert('用户不存在！请检查用户名是否正确');
            return;
        }
    }

    // 🔥 检查用户是否已在列表中（使用严格比较）
    const existingIndex = managerUsers.findIndex(u => String(u) === String(finalUserId));
    if (existingIndex >= 0) {
        await customAlert('该用户已在管理列表中');
        return;
    }

    // 🔥 检查用户是否已被其他管理员管理（全局唯一性检查）
    try {
        const checkResp = await fetch(`${API_BASE_URL}/admin/check-user-assignment?user_id=${finalUserId}`);
        if (checkResp.ok) {
            const checkData = await checkResp.json();
            if (checkData.success && checkData.assigned && String(checkData.manager_id) !== String(currentManagerId)) {
                await customAlert(`该用户已被管理员 ${checkData.manager_id} 管理，无法重复添加`);
                return;
            }
        }
    } catch (error) {
    }

    managerUsers.push(finalUserId);

    // 🔥 保存到数据库
    try {
        const account = adminAccounts.find(a => a.id === currentManagerId);
        if (account) {
            account.users = managerUsers;

            // 更新user_groups（保持现有服务器分配，确保所有managerUsers都有对应的group）
            // 🔥 记录用户添加时间，用于业绩计算
            const existingGroups = managerUserGroups || [];
            const now = new Date().toISOString();
            const updatedUserGroups = managerUsers.map(userId => {
                const existingGroup = existingGroups.find(g => g.userId === userId);
                // 如果是新添加的用户，记录添加时间；如果已存在，保留原有添加时间
                const addedAt = existingGroup && existingGroup.added_at
                    ? existingGroup.added_at
                    : (String(userId) === String(finalUserId) ? now : null);
                return {
                    userId: userId,
                    servers: existingGroup ? (existingGroup.servers || []) : [],
                    added_at: addedAt || now  // 确保所有用户都有添加时间
                };
            });

            // 调用API保存到数据库
            const response = await fetch(`${API_BASE_URL}/admin/account/${currentManagerId}`, {
                method: 'PUT',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    user_groups: updatedUserGroups
                })
            });

            if (!response.ok) {
                const errorData = await response.json().catch(() => ({}));
                await customAlert(`保存失败：${errorData.message || response.statusText || '未知错误'}`);
                // 回滚：从managerUsers中移除刚添加的用户
                managerUsers = managerUsers.filter(u => u !== finalUserId);
                return;
            }

            // API保存成功，更新本地user_groups
            managerUserGroups = updatedUserGroups;
            try {
                StorageManager.admin.setAdminAccounts(adminAccounts);
            } catch (error) {
            }
        }
    } catch (error) {
        await customAlert(`保存失败：${error.message}`);
        // 回滚：从managerUsers中移除刚添加的用户
        managerUsers = managerUsers.filter(u => u !== finalUserId);
        return;
    }

    closeAddUserModal();
    updateManagerDisplay();
    await loadManagerPerformance();
}

/** 添加用户入口函数 */
async function addUser() {
    await showAddUserModal();
}

/** 从管理列表中移除用户 */
async function removeUser(userId) {
    const confirmed = await customConfirm(`确定要移除用户 ${userId} 吗？`);
    if (!confirmed) {
        return;
    }

    // 🔥 移除用户前，先取消该用户的所有服务器分配
    const group = managerUserGroups.find(g => String(g.userId) === String(userId));
    if (group && group.servers && group.servers.length > 0) {
        const allServers = [
            ...serverData.connected,
            ...serverData.disconnected
        ];

        for (const serverName of group.servers) {
            const server = allServers.find(s => s.name === serverName);
            // 即使本地没找到server对象（极少见），也要尝试清理（如果有ID的话）
            // 这里主要依赖本地serverData找到ID
            if (server && server.server_id) {
                try {
                    await fetch(`${API_BASE_URL}/servers/${server.server_id}/unassign`, {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' }
                    });
                } catch (error) {
                }
            }
        }
    }

    managerUsers = managerUsers.filter(u => String(u) !== String(userId));
    managerUserGroups = managerUserGroups.filter(g => String(g.userId) !== String(userId));

    // 🔥 保存到数据库
    const account = adminAccounts.find(a => a.id === currentManagerId);
    if (account) {
        account.users = managerUsers;
        account.userGroups = managerUserGroups;

        try {
            // 更新user_groups到数据库
            const response = await fetch(`${API_BASE_URL}/admin/account/${currentManagerId}`, {
                method: 'PUT',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    user_groups: managerUserGroups
                })
            });

            if (!response.ok) {
            }

            // 保存到存储
            if (typeof setAdminAccounts === 'function') {
                setAdminAccounts(adminAccounts);
            }
        } catch (error) {
        }
    }

    // 重新加载服务器列表以更新状态
    await loadServersFromAPI();
    updateManagerDisplay();
    await loadManagerPerformance();
}

//#endregion
//#region 用户分组管理

/** 创建新的用户分组 */
function createGroup() {
    if (managerUsers.length === 0) {
        customAlert('请先添加用户');
        return;
    }

    currentGroupCreation = {
        userId: null,
        selectedServers: [],
        showingServers: false
    };

    updateManagerDisplay();
}

/** 为分组选择用户 */
function selectUserForGroup(userId) {
    if (!currentGroupCreation) return;
    currentGroupCreation.userId = userId;
    updateManagerDisplay();
}

/** 切换服务器选择面板显示 */
function showServerSelection() {
    if (!currentGroupCreation) return;
    currentGroupCreation.showingServers = !currentGroupCreation.showingServers;
    updateManagerDisplay();
}

/** 切换分组中的服务器选择 */
function toggleServerForGroup(serverName) {
    if (!currentGroupCreation) return;

    const index = currentGroupCreation.selectedServers.indexOf(serverName);
    if (index > -1) {
        currentGroupCreation.selectedServers.splice(index, 1);
    } else {
        currentGroupCreation.selectedServers.push(serverName);
    }
    updateManagerDisplay();
}

/** 确认创建用户分组 */
async function confirmGroupCreation() {
    if (!currentGroupCreation || !currentGroupCreation.userId) {
        await customAlert('请选择用户');
        return;
    }

    if (currentGroupCreation.selectedServers.length === 0) {
        await customAlert('请至少选择一个服务器');
        return;
    }

    const allServers = [
        ...serverData.connected,
        ...serverData.disconnected
    ];

    for (const serverName of currentGroupCreation.selectedServers) {
        const server = allServers.find(s => s.name === serverName);
        if (server && server.server_id) {
            try {
                const response = await fetch(`${API_BASE_URL}/servers/${server.server_id}/assign`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        user_id: currentGroupCreation.userId
                    })
                });
                if (!response.ok) {
                }
            } catch (error) {
            }
        }
    }

    const existingGroup = managerUserGroups.find(g => g.userId === currentGroupCreation.userId);
    if (existingGroup) {
        existingGroup.servers = [...currentGroupCreation.selectedServers];
    } else {
        managerUserGroups.push({
            userId: currentGroupCreation.userId,
            servers: [...currentGroupCreation.selectedServers]
        });
    }

    // 保存到存储
    const account = adminAccounts.find(a => a.id === currentManagerId);
    if (account) {
        account.users = managerUsers;
        account.userGroups = managerUserGroups;
        if (typeof setAdminAccounts === 'function') {
            setAdminAccounts(adminAccounts);
        }
    }

    currentGroupCreation = null;
    await loadServersFromAPI();
    updateManagerDisplay();
}

/** 重置分组创建状态 */
function resetGroupCreation() {
    currentGroupCreation = null;
    updateManagerDisplay();
}

/** 管理现有用户分组 */
function manageUserGroup(userId) {
    const group = managerUserGroups.find(g => g.userId === userId);
    if (!group) return;

    currentGroupCreation = {
        userId: userId,
        selectedServers: [...group.servers],
        showingServers: false
    };
    updateManagerDisplay();
}

/** 删除用户分组 */
async function deleteUserGroup(userId) {
    const group = managerUserGroups.find(g => g.userId === userId);
    if (group) {
        const allServers = [
            ...serverData.connected,
            ...serverData.disconnected
        ];

        for (const serverName of group.servers) {
            const server = allServers.find(s => s.name === serverName);
            if (server && server.server_id) {
                try {
                    await fetch(`${API_BASE_URL}/servers/${server.server_id}/unassign`, {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' }
                    });
                } catch (error) {
                }
            }
        }
    }

    managerUserGroups = managerUserGroups.filter(g => g.userId !== userId);
    await loadServersFromAPI();
    updateManagerDisplay();
}

//#endregion
//#region 业绩统计与用户数据

/** 加载管理员业绩统计数据 */
async function loadManagerPerformance() {
    if (!currentManagerId) return;

    try {
        // 调用单个API获取业绩统计数据（API层处理所有数据计算）
        const response = await fetch(`${API_BASE_URL}/admin/manager/${currentManagerId}/performance`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                users: managerUsers,
                user_groups: managerUserGroups
            })
        }).catch(error => {
            // 捕获网络错误（包括CORS错误）
            return null;
        });

        if (!response || !response.ok) {
            if (!response) {
            } else {
                throw new Error(`API响应错误: ${response.status}`);
            }
            return;
        }

        const data = await response.json();
        if (!data.success) {
            throw new Error(data.message || '获取业绩数据失败');
        }

        // 直接使用API返回的数据，不进行任何计算
        const totalCredits = data.total_credits || 0;
        const userPerformanceData = data.users || [];

        const totalPerformanceDisplay = document.getElementById('totalPerformanceDisplay');
        if (totalPerformanceDisplay) {
            totalPerformanceDisplay.textContent = totalCredits.toFixed(2);
        }

        const container = document.getElementById('performanceBriefContainer');
        if (!container) {
            // 某些页面/布局下该容器不存在，直接跳过即可（避免整页报错）
            return;
        }
        container.innerHTML = '';

        userPerformanceData.forEach((item, index) => {
            if (index % 2 === 0) {
                const row = document.createElement('div');
                row.style.display = 'flex';
                row.style.gap = '10px';
                row.style.width = '100%';
                row.style.marginBottom = '10px';

                const card1 = createPerformanceCard(item.user_id, item.credits);
                row.appendChild(card1);

                if (index + 1 < userPerformanceData.length) {
                    const nextItem = userPerformanceData[index + 1];
                    const card2 = createPerformanceCard(nextItem.user_id, nextItem.credits);
                    row.appendChild(card2);
                } else {
                    const placeholder = document.createElement('div');
                    placeholder.style.flex = '1';
                    row.appendChild(placeholder);
                }

                container.appendChild(row);
            }
        });
    } catch (error) {
    }
}

/** 创建业绩卡片元素 */
function createPerformanceCard(userId, credits) {
    const card = document.createElement('div');
    card.style.cssText = `
        flex: 1;
        padding: 12px;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 50%, #f093fb 100%);
        border-radius: 12px;
        border: 2px solid var(--border-dark);
        color: white;
        font-weight: bold;
        box-shadow: 0 4px 8px rgba(0, 0, 0, 0.2);
    `;
    card.innerHTML = `
        <div style="font-size: 14px; margin-bottom: 5px;">${userId}</div>
        <div style="font-size: 16px; color: #ffd700;">${credits.toFixed(2)} 积分</div>
    `;
    return card;
}

/** 从API获取用户详细数据 */
async function fetchUserData(userId) {
    try {
        const toNum = (v) => {
            const n = Number(v);
            return Number.isFinite(n) ? n : 0;
        };
        const pickTs = (obj) => (obj && (obj.timestamp || obj.ts || obj.created || obj.updated)) || '';
        const normalizeStatLog = (raw) => {
            if (!raw || typeof raw !== 'object') return null;
            const success = toNum(raw.success_count ?? raw.success);
            const fail = toNum(raw.fail_count ?? raw.fail);
            const totalSent = toNum(raw.total_sent ?? raw.sent ?? (success + fail));
            const sentCount = toNum(raw.sent_count ?? raw.task_count ?? (totalSent > 0 ? 1 : 0));
            const credits = toNum(raw.credits ?? raw.amount);
            const ts = pickTs(raw);
            if (!ts && totalSent <= 0 && sentCount <= 0 && credits <= 0) return null;
            const successRate = totalSent > 0 ? (success / totalSent) * 100 : 0;
            return {
                timestamp: ts,
                ts: ts,
                task_count: sentCount,
                sent_count: sentCount,
                total_sent: totalSent,
                success_count: success,
                fail_count: fail,
                credits: credits,
                success_rate: successRate
            };
        };

        const response = await fetch(`${API_BASE_URL}/admin/user/${userId}/summary`, {
            headers: { 'Content-Type': 'application/json' }
        });

        if (!response.ok) {
            throw new Error(`API响应错误: ${response.status}`);
        }

        const data = await response.json();
        if (!data.success) {
            throw new Error(data.message || '获取用户数据失败');
        }

        let usageLogs = Array.isArray(data.usage_logs) ? data.usage_logs : [];
        let lastAccess = data.last_access || '未知';
        let sendRate = toNum(data.send_rate ?? data.rate);

        let lastTaskCount = toNum(data.last_task_count);
        let lastSentCount = toNum(data.last_sent_count);
        let lastSuccessRate = toNum(data.last_success_rate);
        let lastCreditsUsed = toNum(data.last_credits_used);

        let totalAccessCount = toNum(data.total_access_count);
        let totalSentCount = toNum(data.total_sent_count);
        let totalSentAmount = toNum(data.total_sent_amount);
        let totalCreditsUsed = toNum(data.total_credits_used);
        let totalSuccessRate = toNum(data.total_success_rate);

        let usageForRate = [];
        const token = window.authToken || StorageManager.session.getUserToken();
        try {
            const statResponse = await fetch(`${API_BASE_URL}/user/${encodeURIComponent(userId)}/statistics`, {
                method: 'GET',
                headers: token
                    ? { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` }
                    : { 'Content-Type': 'application/json' }
            });
            if (statResponse.ok) {
                const statData = await statResponse.json().catch(() => null);
                if (statData && statData.success) {
                    const statsList = Array.isArray(statData.stats) ? statData.stats : [];
                    const usageList = Array.isArray(statData.usage) ? statData.usage : [];
                    const taskFinishLogs = usageList.filter(item =>
                        item && typeof item === 'object' && String(item.action || '').toLowerCase() === 'task_finish'
                    );
                    usageForRate = taskFinishLogs;

                    if ((!usageLogs || usageLogs.length === 0) && statsList.length > 0) {
                        usageLogs = statsList;
                    }

                    if ((!usageLogs || usageLogs.length === 0) && taskFinishLogs.length > 0) {
                        usageLogs = taskFinishLogs.map(item => ({
                            timestamp: item.ts || item.timestamp || item.created || item.updated,
                            sent_count: 1,
                            total_sent: toNum(item.success) + toNum(item.fail),
                            success_count: toNum(item.success),
                            fail_count: toNum(item.fail),
                            credits: toNum(item.credits),
                            task_count: 1
                        }));
                    }

                    if ((!lastAccess || lastAccess === '未知') && usageList.length > 0) {
                        const latest = usageList
                            .map(item => ({ ts: new Date(pickTs(item)).getTime() }))
                            .filter(x => Number.isFinite(x.ts) && x.ts > 0)
                            .sort((a, b) => b.ts - a.ts)[0];
                        if (latest) {
                            lastAccess = new Date(latest.ts).toLocaleString('zh-CN', { hour12: false });
                        }
                    }
                }
            }
        } catch {
            // ignore fallback api failure
        }

        const normalizedUsageLogs = (Array.isArray(usageLogs) ? usageLogs : [])
            .map(normalizeStatLog)
            .filter(Boolean)
            .sort((a, b) => new Date(pickTs(a)).getTime() - new Date(pickTs(b)).getTime());

        if (normalizedUsageLogs.length > 0) {
            usageLogs = normalizedUsageLogs;
            const aggSentCount = normalizedUsageLogs.reduce((sum, row) => sum + toNum(row.sent_count), 0);
            const aggSentAmount = normalizedUsageLogs.reduce((sum, row) => sum + toNum(row.total_sent), 0);
            const aggSuccess = normalizedUsageLogs.reduce((sum, row) => sum + toNum(row.success_count), 0);
            const aggCredits = normalizedUsageLogs.reduce((sum, row) => sum + toNum(row.credits), 0);

            if (totalSentCount <= 0) totalSentCount = aggSentCount;
            if (totalSentAmount <= 0) totalSentAmount = aggSentAmount;
            if (totalCreditsUsed <= 0) totalCreditsUsed = aggCredits;
            if (totalAccessCount <= 0) totalAccessCount = normalizedUsageLogs.length;
            if (totalSuccessRate <= 0 && aggSentAmount > 0) {
                totalSuccessRate = (aggSuccess / aggSentAmount) * 100;
            }

            const lastLog = normalizedUsageLogs[normalizedUsageLogs.length - 1];
            if (lastTaskCount <= 0) lastTaskCount = toNum(lastLog.task_count ?? lastLog.sent_count);
            if (lastSentCount <= 0) lastSentCount = toNum(lastLog.total_sent);
            if (lastSuccessRate <= 0) lastSuccessRate = toNum(lastLog.success_rate);
            if (lastCreditsUsed <= 0) lastCreditsUsed = toNum(lastLog.credits);
            if (!lastAccess || lastAccess === '未知') {
                const ts = pickTs(lastLog);
                if (ts) lastAccess = ts;
            }
        } else {
            usageLogs = [];
        }

        if (sendRate <= 0 && usageForRate.length > 0) {
            const recent = [...usageForRate].reverse().find(item =>
                toNum(item.credits) > 0 && (toNum(item.success) > 0 || (toNum(item.success) + toNum(item.fail)) > 0)
            );
            if (recent) {
                const success = toNum(recent.success);
                const fail = toNum(recent.fail);
                const denominator = success > 0 ? success : (success + fail);
                if (denominator > 0) {
                    sendRate = toNum(recent.credits) / denominator;
                }
            }
        }

        if ((!lastAccess || lastAccess === '未知') && String(userId) === String(window.currentUserId || StorageManager.session.getUserId() || '')) {
            const loginTime = StorageManager.session.getLoginTime ? StorageManager.session.getLoginTime() : null;
            if (loginTime && Number.isFinite(loginTime)) {
                lastAccess = new Date(loginTime).toLocaleString('zh-CN', { hour12: false });
            }
        }

        return {
            userId: data.user_id || userId,
            username: data.username || '',
            credits: toNum(data.credits),
            created: data.created || '未知',
            lastAccess: lastAccess,
            lastTaskCount: lastTaskCount,
            lastSuccessRate: lastSuccessRate,
            lastSentCount: lastSentCount,
            lastCreditsUsed: lastCreditsUsed,
            totalAccessCount: totalAccessCount,
            totalSentCount: totalSentCount,
            totalSentAmount: totalSentAmount,
            totalCreditsUsed: totalCreditsUsed,
            totalSuccessRate: totalSuccessRate,
            sendRate: sendRate > 0 ? Number(sendRate.toFixed(4)) : 0,
            usage_logs: usageLogs,
            consumption_logs: data.consumption_logs || [],
            recharge_logs: data.recharge_logs || []
        };
    } catch (error) {
        return {
            userId: userId,
            username: '',
            credits: 0,
            created: '未知',
            lastAccess: '未知',
            lastTaskCount: 0,
            lastSuccessRate: 0,
            lastSentCount: 0,
            lastCreditsUsed: 0,
            totalAccessCount: 0,
            totalSentCount: 0,
            totalSentAmount: 0,
            totalCreditsUsed: 0,
            totalSuccessRate: 0,
            sendRate: 0,
            usage_logs: [],
            consumption_logs: [],
            recharge_logs: []
        };
    }
}

/** 生成用户详情内容HTML */
function generateUserDetailContent(userData, userId, showServerSection = true) {
    let serverSectionHtml = '';

    if (showServerSection) {
        const userGroup = managerUserGroups.find(g => g.userId === userId);
        const assignedServers = userGroup ? userGroup.servers : [];

        const account = adminAccounts.find(a => a.id === currentManagerId);
        const managerAssignedServers = account && account.selectedServers ? account.selectedServers : [];
        const liveServers = [...serverData.connected];

        serverSectionHtml = `
            <div class="user-detail-server-section">
                <div class="user-detail-server-header">
                    <div style="font-size: 14px; font-weight: bold;">分配私有服务器</div>
                    <div class="user-detail-server-hint" style="font-size: 11px;"> 分配服务器仅供指定用户使用  获得私有号码  开通双向发送 </div>
                </div>
        
                <div id="userServerSelectionGrid" class="server-buttons-grid">
                    ${managerAssignedServers.map(serverName => {
            const s = liveServers.find(x => String(x.name).trim() === String(serverName).trim());
            const url = s ? (s.url || '') : '';
            const portMatch = url.match(/:(\d+)/);
            const port = portMatch ? portMatch[1] : (s && (s.port || (String(s.name).match(/\d+/)?.[0]))) || '?';
            const safeUserId = String(userId).replace(/'/g, "\\'");
            const safeServerName = String(serverName).replace(/'/g, "\\'");

            // 逻辑判断
            const isAssignedToCurrentUser = assignedServers.includes(serverName);

            // 检查是否分配给了其他用户
            let assignedToOtherUserId = null;
            for (const group of managerUserGroups) {
                if (String(group.userId) !== String(userId) && group.servers.includes(serverName)) {
                    assignedToOtherUserId = group.userId;
                    break;
                }
            }

            const botHTML = SERVER_BOT_HTML;

            let buttonClass = 'server-button connected';
            let statusText = '状态: 可分配';
            let statusColor = '#00ff88';
            let onClick = `onclick="toggleUserServerSelection('${safeUserId}', '${safeServerName}', this)"`;
            let extraTooltip = '';
            let nameColor = '#2d3436';

            if (assignedToOtherUserId) {
                // 分配给其他用户 -> 私享VIP状态 (不可选)
                buttonClass += ' private disabled';
                statusText = '状态: 私享 (不可选)';
                statusColor = '#ff0080';
                nameColor = '#ff0080';
                onClick = ''; // 禁用点击
                extraTooltip = `<div style="font-size: 11px; color: #ff0080; margin-top: 4px; font-weight: bold; text-shadow: 0 0 5px rgba(255,0,128,0.5);">私享服务器: ${assignedToOtherUserId}</div>`;
            } else if (isAssignedToCurrentUser) {
                // 分配给当前用户 -> 选中状态 (且显示为私享效果)
                // 添加 private 类以激活 VIP 彩虹/流光特效，但保留 selected 类以此表明选中状态，且不禁用点击
                buttonClass += ' selected private';
                statusText = '状态: 已选中 (VIP)';
                statusColor = '#ffd700';
                nameColor = '#d63031';
            }

            return `<button class="${buttonClass}" ${onClick}>
                                ${botHTML}
                                <div class="server-button-name" style="position: absolute; bottom: -15px; left: 50%; transform: translateX(-50%); font-size: 11px; color: ${nameColor}; white-space: nowrap; pointer-events: none; z-index: 100;">${serverName}</div>
                                <div class="server-tooltip">
                                    <div style="font-weight: bold; margin-bottom: 4px;">${serverName}</div>
                                    <div style="font-size: 11px; opacity: 0.9;">${url || ''}</div>
                                    <div style="font-size: 11px; color: ${statusColor}; margin-top: 4px;" class="status-text">${statusText}</div>
                                    ${extraTooltip}
                                </div>
                            </button>`;
        }).join('')}
                </div>
                <div class="user-detail-footer" style="display: flex; justify-content: center; gap: 70px; margin-top: 20px;">
                    <button class="admin-manage-footer-btn reset" onclick="resetUserServerSelection('${userId}')" style="width: 70px;">重置</button>
                    <button class="admin-manage-footer-btn confirm" onclick="confirmUserServerSelection('${userId}')" style="width: 70px;">确定</button>
                </div>
            </div>
        `;
    }

    // 处理用户ID：如果是u_格式，提取4位数字；否则直接使用（已经是纯4位数字）
    let userIdDisplay = String(userData.userId || '');
    if (userIdDisplay.startsWith('u_')) {
        userIdDisplay = userIdDisplay.substring(2);
    }
    const usernameDisplay = userData.username || '未设置';

    // 格式化日期显示（月/日）
    function formatDateForDisplay(dateStr) {
        const date = new Date(dateStr);
        const month = (date.getMonth() + 1).toString().padStart(2, '0');
        const day = date.getDate().toString().padStart(2, '0');
        return `${month}/${day}`;
    }

    // 处理使用记录：按天分组
    const usageLogs = userData.usage_logs || [];
    const dailyRecords = {};
    usageLogs.forEach(log => {
        const ts = log.timestamp || log.ts || log.created;
        if (!ts) return;
        const date = new Date(ts);
        const dateKey = date.toISOString().split('T')[0]; // YYYY-MM-DD
        if (!dailyRecords[dateKey]) {
            dailyRecords[dateKey] = {
                date: dateKey,
                sentCount: 0,
                totalAmount: 0,
                success: 0,
                fail: 0,
                creditsUsed: 0
            };
        }
        dailyRecords[dateKey].sentCount += log.sent_count || 0;
        dailyRecords[dateKey].totalAmount += log.total_sent || 0;
        dailyRecords[dateKey].success += log.success_count || 0;
        dailyRecords[dateKey].fail += log.fail_count || 0;
        dailyRecords[dateKey].creditsUsed += log.credits || log.amount || 0;
    });

    // 转换为数组并按日期倒序排列
    const sortedDailyRecords = Object.values(dailyRecords)
        .sort((a, b) => new Date(b.date) - new Date(a.date))
        .map(record => {
            const successRate = record.totalAmount > 0
                ? ((record.success / record.totalAmount) * 100).toFixed(2)
                : '0.00';
            return {
                ...record,
                successRate: successRate,
                dateDisplay: formatDateForDisplay(record.date)
            };
        });

    // 计算总记录
    const totalRecord = {
        sentCount: userData.totalSentCount || 0,
        totalAmount: userData.totalSentAmount || 0,
        success: sortedDailyRecords.reduce((sum, r) => sum + r.success, 0),
        fail: sortedDailyRecords.reduce((sum, r) => sum + r.fail, 0),
        creditsUsed: userData.totalCreditsUsed || 0
    };
    const totalSuccessRate = totalRecord.totalAmount > 0
        ? ((totalRecord.success / totalRecord.totalAmount) * 100).toFixed(2)
        : '0.00';

    // 🔥 充值记录（用于生成HTML）- 使用recharge_logs，不是consumption_logs
    const rechargeRecordsForHTML = userData.recharge_logs || [];
    let rechargeHTML = '';
    if (!rechargeRecordsForHTML || rechargeRecordsForHTML.length === 0) {
        rechargeHTML = '<div style="padding: 15px; text-align: center; color: #999;">暂无充值记录</div>';
    } else {
        const sortedRechargeRecords = rechargeRecordsForHTML.sort((a, b) => {
            const timeA = new Date(a.ts || 0).getTime();
            const timeB = new Date(b.ts || 0).getTime();
            return timeB - timeA;
        });

        rechargeHTML = '<div style="display: grid; grid-template-columns: 80px 1fr 200px 150px; gap: 15px; padding: 12px 15px; background: #f9f9f9; font-weight: bold; border-bottom: 2px solid #ddd; font-size: 14px; position: sticky; top: 0; z-index: 10;">';
        rechargeHTML += '<div>记录</div><div>用户</div><div>时间</div><div style="text-align: right;">充值金额</div></div>';

        sortedRechargeRecords.forEach((record, index) => {
            const time = record.ts ? new Date(record.ts).toLocaleString('zh-CN', {
                year: 'numeric',
                month: '2-digit',
                day: '2-digit',
                hour: '2-digit',
                minute: '2-digit'
            }) : '-';
            const amount = parseFloat(record.amount || 0).toFixed(2);
            const bgColor = index % 2 === 0 ? '#fff' : '#f9f9f9';
            rechargeHTML += `<div style="display: grid; grid-template-columns: 80px 1fr 200px 150px; gap: 15px; padding: 12px 15px; background: ${bgColor}; border-bottom: 1px solid #eee; font-size: 14px; align-items: center;">`;
            rechargeHTML += `<div style="color: #666;">${index + 1}</div>`;
            rechargeHTML += `<div style="color: #333;">${usernameDisplay}</div>`;
            rechargeHTML += `<div style="color: #666; font-size: 13px;">${time}</div>`;
            rechargeHTML += `<div style="color: #4CAF50; font-weight: bold; text-align: right;">+${amount}</div>`;
            rechargeHTML += '</div>';
        });
    }

    // 🔥 计算充值总额度 - 使用recharge_logs
    const rechargeRecords = userData.recharge_logs || [];
    const totalRechargeAmount = rechargeRecords.reduce((sum, record) => {
        return sum + parseFloat(record.amount || 0);
    }, 0);
    const displayUserRate = Number(userData.sendRate) > 0
        ? Number(userData.sendRate).toFixed(4).replace(/\.?0+$/, '')
        : '未知';

    return `
        <div class="user-detail-header" style="display: flex; align-items: center; gap: 15px; flex-wrap: nowrap;">
            <div style="font-size: 16px; font-weight: bold; white-space: nowrap;">用户名: ${usernameDisplay}</div>
            <div style="font-size: 14px; color: #666; white-space: nowrap;">用户ID: ${userIdDisplay}</div>
            <div style="font-size: 14px; color: #666; white-space: nowrap;">上次登录时间: ${userData.lastAccess || '未知'}</div>
            <div style="margin-left: auto; display: flex; gap: 15px; align-items: center; flex-shrink: 0;">
                <div style="display: flex; align-items: center; gap: 8px; padding: 8px 15px; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); border-radius: 8px; box-shadow: 0 2px 8px rgba(102, 126, 234, 0.3);">
                    <span style="font-size: 14px; color: white; font-weight: bold;">充值总额度:</span>
                    <span style="font-size: 16px; color: white; font-weight: bold;">${totalRechargeAmount.toFixed(2)}</span>
                </div>
                <div style="display: flex; align-items: center; gap: 8px; padding: 8px 15px; background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%); border-radius: 8px; box-shadow: 0 2px 8px rgba(79, 172, 254, 0.3);">
                    <span style="font-size: 14px; color: white; font-weight: bold;">积分余额:</span>
                    <span style="font-size: 16px; color: white; font-weight: bold;">${userData.credits.toFixed(2)}</span>
                </div>
            </div>
        </div>

        </div>

        <div style="margin-top: 15px; padding: 12px 15px; background: #fff; border: 1px solid #ddd; border-radius: 8px; display: flex; align-items: center;">
            <div style="font-size: 14px; font-weight: bold; color: #333;">用户费率: <span style="color: #2196F3;">${displayUserRate}</span></div>
        </div>

        <div style="margin-top: 20px;">
            <div style="font-size: 14px; font-weight: bold; margin-bottom: 10px;">用户统计数据</div>
            <div style="background: white; border: 1px solid #ddd; border-radius: 8px; overflow: hidden; max-height: 400px; overflow-y: auto;">
                <!-- 第一行：标题 -->
                <div style="display: grid; grid-template-columns: 100px 100px 120px 100px 120px 100px; gap: 10px; padding: 10px 15px; background: #f9f9f9; font-weight: bold; border-bottom: 2px solid #ddd; font-size: 13px; position: sticky; top: 0; z-index: 10;">
                    <div>发送次数</div>
                    <div>总数量</div>
                    <div>成功/失败</div>
                    <div>成功率: %</div>
                    <div>总消费:</div>
                    <div>日期</div>
                </div>
                <!-- 第二行：总数 (黄色背景，日期留空) -->
                <div style="display: grid; grid-template-columns: 100px 100px 120px 100px 120px 100px; gap: 10px; padding: 10px 15px; background: #ffff00; font-size: 13px; align-items: center; font-weight: bold; border-bottom: 1px solid #ccc;">
                    <div>${totalRecord.sentCount}</div>
                    <div>${totalRecord.totalAmount}</div>
                    <div>${totalRecord.success}/${totalRecord.fail}</div>
                    <div>${totalSuccessRate}%</div>
                    <div style="color: #f44336;">${totalRecord.creditsUsed.toFixed(2)}</div>
                    <div></div>
                </div>
                <!-- 第三行开始：单次记录 (按天，最新在上) -->
                ${sortedDailyRecords.length > 0 ? sortedDailyRecords.map((record, index) => {
            const bgColor = index % 2 === 0 ? '#fff' : '#f9f9f9';
            return `
                        <div style="display: grid; grid-template-columns: 100px 100px 120px 100px 120px 100px; gap: 10px; padding: 10px 15px; background: ${bgColor}; border-bottom: 1px solid #eee; font-size: 13px; align-items: center;">
                            <div>${record.sentCount}</div>
                            <div>${record.totalAmount}</div>
                            <div>${record.success}/${record.fail}</div>
                            <div>${record.successRate}%</div>
                            <div style="color: #666;">${record.creditsUsed.toFixed(2)}</div>
                            <div>${record.dateDisplay}</div>
                        </div>
                    `;
        }).join('') : '<div style="padding: 20px; text-align: center; color: #999;">暂无详细记录</div>'}
            </div>
        </div>

        <div style="margin-top: 20px;">
            <div style="font-size: 14px; font-weight: bold; margin-bottom: 10px; display: flex; justify-content: space-between; align-items: center;">
                <span>充值记录</span>
                ${!showServerSection ? `<button onclick="handleRecharge()" style="padding: 4px 12px; background: linear-gradient(135deg, #FF9800 0%, #FF5722 100%); color: white; border: none; border-radius: 6px; font-size: 12px; cursor: pointer; font-weight: bold; box-shadow: 0 2px 5px rgba(255, 87, 34, 0.3);">充值</button>` : ''}
            </div>
            <div style="background: white; border: 1px solid #ddd; border-radius: 8px; overflow: hidden; max-height: 400px; overflow-y: auto;">
                ${rechargeHTML}
            </div>
        </div>
        ${serverSectionHtml}
    `;
}

//#endregion
//#region 用户详情弹窗

/** 显示用户详情弹窗 */
async function showUserDetailModal(userId) {
    const modal = document.getElementById('userDetailModal');
    const content = document.getElementById('userDetailContent');
    if (!modal || !content) return;

    const userData = await fetchUserData(userId);

    content.innerHTML = generateUserDetailContent(userData, userId, true);

    requestAnimationFrame(() => {
        modal.classList.add('show');
    });
}

/** 加载账户面板内容 */
async function loadAccountPanelContent() {
    const panelE = document.getElementById('panelE');
    if (!panelE) return;

    const panelContent = panelE.querySelector('.panel-content');
    if (!panelContent) return;

    // 尝试多种方式获取用户ID
    let userId = window.currentUserId || StorageManager.session.getUserId();
    
    if (!userId) {
        // 如果还是没有用户ID，显示友好的提示
        panelContent.innerHTML = `
            <div style="padding: 40px; text-align: center; color: #666;">
                <div style="font-size: 18px; margin-bottom: 10px;">⚠️ 未检测到登录信息</div>
                <div style="font-size: 14px; margin-bottom: 20px;">请重新登录或刷新页面</div>
                <button onclick="location.reload()" style="padding: 8px 16px; background: #4facfe; color: white; border: none; border-radius: 4px; cursor: pointer;">
                    刷新页面
                </button>
            </div>
        `;
        return;
    }

    panelContent.innerHTML = '<div style="padding: 20px; text-align: center; color: #666;">加载中...</div>';

    try {
        const userData = await fetchUserData(userId);
        panelContent.innerHTML = generateUserDetailContent(userData, userId, false);
    } catch (error) {
        console.error('加载账号信息失败:', error);
        panelContent.innerHTML = `
            <div style="padding: 40px; text-align: center; color: #ff6b6b;">
                <div style="font-size: 18px; margin-bottom: 10px;">❌ 加载失败</div>
                <div style="font-size: 14px; margin-bottom: 20px;">${error.message || '未知错误'}</div>
                <button onclick="loadAccountPanelContent()" style="padding: 8px 16px; background: #4facfe; color: white; border: none; border-radius: 4px; cursor: pointer;">
                    重试
                </button>
            </div>
        `;
    }
}

/** 关闭用户详情弹窗 */
function closeUserDetailModal() {
    const modal = document.getElementById('userDetailModal');
    if (!modal) return;
    modal.classList.remove('show');
}

/** 切换用户服务器选择状态 */
function toggleUserServerSelection(userId, serverName, button) {
    // 如果按钮被禁用（例如是别人的私享服务器），直接返回
    if (button.classList.contains('disabled')) return;

    // 查找或创建用户组
    let userGroup = managerUserGroups.find(g => String(g.userId) === String(userId));
    if (!userGroup) {
        userGroup = {
            userId: userId,
            servers: []
        };
        managerUserGroups.push(userGroup);
    }

    const index = userGroup.servers.indexOf(serverName);
    const nameEl = button.querySelector('.server-button-name');
    const statusText = button.querySelector('.status-text');

    if (index > -1) {
        // 已存在 -> 移除
        userGroup.servers.splice(index, 1);
        button.classList.remove('selected', 'private'); // 移除选中和VIP特效
        if (statusText) {
            statusText.textContent = '状态: 可分配';
            statusText.style.color = '#00ff88';
        }
        if (nameEl) nameEl.style.color = '#2d3436';
    } else {
        // 不存在 -> 添加
        userGroup.servers.push(serverName);
        button.classList.add('selected', 'private'); // 添加选中和VIP特效
        if (statusText) {
            statusText.textContent = '状态: 已选中 (VIP)';
            statusText.style.color = '#ffd700';
        }
        if (nameEl) nameEl.style.color = '#d63031';
    }
}

/** 重置用户服务器选择 */
function resetUserServerSelection(userId) {
    const userGroup = managerUserGroups.find(g => g.userId === userId);
    if (userGroup) {
        userGroup.servers = [];
    }
    const grid = document.getElementById('userServerSelectionGrid');
    if (grid) {
        const buttons = grid.querySelectorAll('.server-button');
        buttons.forEach(btn => btn.classList.remove('selected'));
    }
}

/** 确认用户服务器选择并保存 */
async function confirmUserServerSelection(userId) {
    const userGroup = managerUserGroups.find(g => g.userId === userId);
    const selectedServers = userGroup ? userGroup.servers : [];

    const allServers = [
        ...serverData.connected,
        ...serverData.disconnected
    ];

    for (const serverName of selectedServers) {
        const server = allServers.find(s => s.name === serverName);
        if (server && server.server_id) {
            try {
                const response = await fetch(`${API_BASE_URL}/servers/${server.server_id}/assign`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        user_id: userId
                    })
                });
                if (!response.ok) {
                    const err = await response.json().catch(() => ({}));
                }
            } catch (error) {
            }
        }
    }

    const account = adminAccounts.find(a => a.id === currentManagerId);
    const managerAssignedServers = account && account.selectedServers ? account.selectedServers : [];
    for (const serverName of managerAssignedServers) {
        if (!selectedServers.includes(serverName)) {
            const server = allServers.find(s => s.name === serverName);
            if (server && server.server_id) {
                try {
                    await fetch(`${API_BASE_URL}/servers/${server.server_id}/unassign`, {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' }
                    });
                } catch (error) {
                }
            }
        }
    }

    await loadServersFromAPI();
    closeUserDetailModal();
    updateManagerDisplay();
    await customAlert('服务器分配已保存');
}

//#endregion
//#region 费率设置

/** 显示费率编辑器并加载费率范围 */
async function showRateEditor(userId) {
    const editor = document.getElementById('rateEditor_' + userId);
    const hint = document.getElementById('rateRangeHint_' + userId);
    
    if (!editor) return;
    
    editor.style.display = 'flex';
    hint.textContent = '加载费率范围...';
    
    try {
        const adminToken = StorageManager.session.getAdminToken();
        if (!adminToken) {
            hint.textContent = '❌ 未找到管理员token';
            return;
        }
        
        const mgrId = StorageManager.session.getCurrentManagerId() || currentManagerId;
        if (!mgrId) {
            hint.textContent = '❌ 未找到管理员ID';
            return;
        }
        
        // 调用API获取管理员费率范围
        const res = await fetch(`${API_BASE_URL}/admin/rates/admin-range?admin_id=${mgrId}`, {
            headers: {
                'Authorization': `Bearer ${adminToken}`
            }
        });
        
        const data = await res.json();
        if (data.success && data.rate_range) {
            const min = data.rate_range.min.toFixed(4);
            const max = data.rate_range.max.toFixed(4);
            hint.textContent = `可设置范围: ${min} - ${max}`;
            // 存储范围供saveUserCustomRateFromEditor使用
            editor.dataset.minRate = min;
            editor.dataset.maxRate = max;
        } else {
            hint.textContent = '❌ 费率范围未设置，请联系超级管理员';
            editor.dataset.minRate = '';
            editor.dataset.maxRate = '';
        }
    } catch (e) {
        hint.textContent = '❌ 加载失败: ' + e.message;
        editor.dataset.minRate = '';
        editor.dataset.maxRate = '';
    }
}

/** 从编辑器保存用户费率 */
async function saveUserCustomRateFromEditor(userId) {
    const editor = document.getElementById('rateEditor_' + userId);
    if (!editor) return;
    
    const min = editor.dataset.minRate;
    const max = editor.dataset.maxRate;
    
    if (!min || !max) {
        await customAlert('❌ 费率范围未加载，请稍后再试');
        return;
    }
    
    await saveUserCustomRate(userId, min, max);
}

/** 保存用户自定义费率 */
async function saveUserCustomRate(userId, min, max) {
    const input = document.getElementById('newRate_' + userId);
    if (!input) return;
    
    const rateValue = input.value.trim();
    if (!rateValue) {
        // 如果为空，询问是否清除费率
        if (!confirm('确定要清除该用户的费率设置吗？（将恢复使用全局费率）')) {
            return;
        }
    }
    
    const rate = rateValue ? parseFloat(rateValue) : null;
    if (rateValue && isNaN(rate)) {
        await customAlert('请输入有效的费率（数字）');
        return;
    }
    
    if (rate !== null) {
        // 验证费率范围（保留4位小数）
        const rateRounded = Math.round(rate * 10000) / 10000;
        const minRate = parseFloat(min);
        const maxRate = parseFloat(max);
        
        if (rateRounded < minRate || rateRounded > maxRate) {
            await customAlert(`费率必须在 ${minRate.toFixed(4)} - ${maxRate.toFixed(4)} 之间`);
            return;
        }
    }

    try {
        const adminToken = StorageManager.session.getAdminToken();
        if (!adminToken) {
            await customAlert('❌ 未找到管理员token，请重新登录');
            return;
        }
        
        // 调用API设置用户费率
        const res = await fetch(`${API_BASE_URL}/admin/rates/user-by-admin`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${adminToken}`
            },
            body: JSON.stringify({
                user_id: userId,
                rates: rate !== null ? { send: rate.toFixed(4) } : null
            })
        });
        
        const data = await res.json();
        if (data.success) {
            await customAlert('✅ 用户费率已更新');
            document.getElementById('rateEditor_' + userId).style.display = 'none';
            input.value = '';
            // 刷新用户列表显示
            updateManagerDisplay();
        } else {
            await customAlert('❌ 保存失败: ' + (data.message || '未知错误'));
            if (data.min !== undefined && data.max !== undefined) {
                await customAlert(`允许的费率范围：${data.min.toFixed(4)} - ${data.max.toFixed(4)}`);
            }
        }
    } catch (e) {
        await customAlert('❌ 网络错误: ' + e.message);
    }
}

//#endregion
//#region 管理界面显示更新

/** 更新管理器显示定时器 */
let updateManagerDisplayTimer = null;

/** 更新管理器界面显示 */
function updateManagerDisplay() {
    if (updateManagerDisplayTimer) {
        clearTimeout(updateManagerDisplayTimer);
    }

    updateManagerDisplayTimer = setTimeout(async () => {
        const userList = document.getElementById('userList');
        if (!userList) return;

        userList.innerHTML = '<div style="padding: 20px; text-align: center; color: #666;">加载中...</div>';

        const managerUserCountDisplay = document.getElementById('managerUserCountDisplay');
        if (managerUserCountDisplay) {
            managerUserCountDisplay.textContent = managerUsers.length;
        }

        // 获取管理员分配的服务器列表
        const account = adminAccounts.find(a => a.id === currentManagerId);
        let managerAssignedServers = [];

        if (account && account.selectedServers) {
            managerAssignedServers = account.selectedServers;
        } else {
            // 如果本地没有，尝试从API获取（异步，不阻塞）
            try {
                const response = await fetch(`${API_BASE_URL}/admin/account/${currentManagerId}`);
                if (response.ok) {
                    const data = await response.json();
                    if (data && data.success && data.selected_servers) {
                        managerAssignedServers = data.selected_servers;
                        if (account) {
                            account.selectedServers = managerAssignedServers;
                            StorageManager.admin.setAdminAccounts(adminAccounts);
                        }
                    }
                }
            } catch (error) {
            }
        }

        // 调用单个API获取所有显示数据（API层处理所有数据计算和服务器筛选）
        try {
            const response = await fetch(`${API_BASE_URL}/admin/manager/${currentManagerId}/display`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    users: managerUsers,
                    user_groups: managerUserGroups,
                    selected_servers: managerAssignedServers
                })
            }).catch(error => {
                // 捕获网络错误（包括CORS错误）
                return null;
            });

            if (!response || !response.ok) {
                if (!response) {
                    userList.innerHTML = '<div style="padding: 20px; text-align: center; color: #ff6b6b;">无法连接到服务器，请检查网络连接或CORS设置</div>';
                } else {
                    throw new Error(`API响应错误: ${response.status}`);
                }
                return;
            }

            const data = await response.json();
            if (!data.success) {
                throw new Error(data.message || '获取显示数据失败');
            }

            // 使用API返回的用户列表渲染用户按钮
            const fragment = document.createDocumentFragment();
            const userListData = data.user_list || [];

            userListData.forEach(userData => {
                const userButton = document.createElement('div');
                userButton.className = 'user-button';
                const fullUserId = String(userData.user_id || '');
                // 处理用户ID：如果是u_格式，提取4位数字；否则直接使用（已经是纯4位数字）
                let userIdDisplay = fullUserId;
                if (fullUserId.startsWith('u_')) {
                    userIdDisplay = fullUserId.substring(2);
                }
                const usernameDisplay = userData.username || '未设置';
                const escapedUserId = fullUserId.replace(/"/g, '&quot;').replace(/'/g, '&#39;');
                userButton.innerHTML = `
                    <div class="user-button-content">
                        <div class="user-server-count-badge ${(userData.server_count || 0) > 0 ? 'flash' : ''}">${userData.server_count || 0}</div>
                        <div class="user-button-info">
                            <div class="user-button-top">
                                <span class="user-id-text">${usernameDisplay}(${userIdDisplay})</span>
                                <div style="display: flex; gap: 5px;">
                                    <button class="user-manage-btn" onclick="showUserDetailModal('${escapedUserId}')">管理</button>
                                    <button class="user-manage-btn" onclick="removeUser('${escapedUserId}')" style="background: #f44336; color: white;">移除</button>
                                </div>
                            </div>
                            <div class="user-button-stats">
                                <div class="user-stat-item">
                                    <span class="user-stat-label">rate:</span>
                                    <span class="user-stat-value">${(userData.send_rate ?? userData.rate ?? '') || '-'}</span>
                                </div>
                                <div class="user-stat-item">
                                    <span class="user-stat-label">balance:</span>
                                    <span class="user-stat-value">$${(userData.credits || 0).toFixed(2)}</span>
                                </div>
                            </div>
                        </div>
                    </div>
                `;
                fragment.appendChild(userButton);
            });

            userList.innerHTML = '';
            userList.appendChild(fragment);


            // 只显示管理员有权限的服务器（selected_servers）
            const availableContainer = document.getElementById('managerAvailableServers');
            if (!availableContainer) {
                return;
            }
            availableContainer.innerHTML = '';

            const serversData = data.servers || {};
            // 🔥 只显示管理员有权限分配的服务器
            const managerAvailableServers = serversData.available || [];

            // 如果没有分配权限，显示提示
            if (!managerAssignedServers || managerAssignedServers.length === 0) {
                availableContainer.innerHTML = '<div style="padding: 20px; text-align: center; color: #999;"></div>';
                return;
            }

            // 过滤出管理员有权限的服务器
            const managerAssignedServersSet = new Set(managerAssignedServers.map(s => String(s).trim()));
            const assignedToUsers = (serversData.assigned || []).filter(s => {
                const serverName = s.name || s.server_name || s.server_id || String(s);
                return managerAssignedServersSet.has(String(serverName).trim());
            });
            const availableForAssignment = managerAvailableServers.filter(s => {
                const serverName = s.name || s.server_name || s.server_id || String(s);
                return managerAssignedServersSet.has(String(serverName).trim());
            });

            // 为了后续代码兼容，需要构建allServers数组（从API返回的数据构建）
            const allServers = [
                ...assignedToUsers,
                ...availableForAssignment
            ].map(s => ({
                name: s.name || s.server_name || s.server_id,
                server_id: s.server_id,
                url: s.url || s.server_url || '',
                port: s.port,
                status: s.status
            }));

            // 构建已分配服务器的集合（用于后续代码）
            const assignedServers = new Set();
            if (data.user_groups) {
                data.user_groups.forEach(group => {
                    if (group.servers) {
                        group.servers.forEach(s => assignedServers.add(String(s)));
                    }
                });
            }

            if (assignedToUsers.length > 0) {
                const assignedSection = document.createElement('div');
                assignedSection.style.marginBottom = '20px';

                const assignedTitle = document.createElement('div');
                assignedTitle.className = 'server-status-header';
                assignedTitle.innerHTML = `已分配给用户 <span class="count">(${assignedToUsers.length})</span>`;
                assignedSection.appendChild(assignedTitle);

                const assignedGrid = document.createElement('div');
                assignedGrid.className = 'server-buttons-grid';

                assignedToUsers.forEach(server => {
                    const btn = document.createElement('button');
                    btn.className = 'server-button connected assigned';
                    const serverName = server.name || server.server_name || server.server_id || String(server);

                    if (currentGroupCreation && currentGroupCreation.selectedServers.includes(serverName)) {
                        btn.classList.add('selected', 'active');
                        btn.onclick = () => {
                            btn.classList.toggle('active');
                            toggleServerForGroup(serverName);
                        };
                    } else {
                        btn.onclick = () => btn.classList.toggle('active');
                    }

                    const portMatch = (server.url || '').match(/:(\d+)/);
                    const port = portMatch ? portMatch[1] : (server.port || serverName.match(/\d+/)?.[0] || '?');
                    const isSelected = currentGroupCreation && currentGroupCreation.selectedServers.includes(serverName);
                    const statusText = isSelected ? '状态: 已选中' : '状态: 已连接';
                    const botHTML = SERVER_BOT_HTML;

                    btn.innerHTML = botHTML + `
                        <div class="server-button-name" style="position: absolute; bottom: -15px; left: 50%; transform: translateX(-50%); font-size: 11px; color: #2d3436; white-space: nowrap; pointer-events: none; z-index: 100;">${serverName}</div>
                        <div class="server-tooltip">
                            <div style="font-weight: bold; margin-bottom: 4px;">${serverName}</div>
                            <div style="font-size: 11px; opacity: 0.9;">${server.url || ''}</div>
                            <div style="font-size: 11px; color: ${isSelected ? '#ffd700' : '#00ff88'}; margin-top: 4px;" class="status-text">${statusText}</div>
                            <div style="font-size: 11px; color: #ff9800; margin-top: 2px;">已分配给用户</div>
                        </div>
                    `;
                    assignedGrid.appendChild(btn);
                });

                assignedSection.appendChild(assignedGrid);
                availableContainer.appendChild(assignedSection);

                // 添加分隔线
                const divider = document.createElement('div');
                divider.className = 'server-status-divider';
                availableContainer.appendChild(divider);

                // 初始化雷达机器人
                initRadarBots();
            }

            if (availableForAssignment.length > 0) {
                const availableSection = document.createElement('div');
                availableSection.style.marginBottom = '20px';



                const availableGrid = document.createElement('div');
                availableGrid.className = 'server-buttons-grid';

                availableForAssignment.forEach(server => {
                    const btn = document.createElement('button');
                    btn.className = 'server-button connected';
                    const serverName = server.name || server.server_name || server.server_id || String(server);

                    if (currentGroupCreation) {
                        const isSelected = currentGroupCreation.selectedServers.includes(serverName);
                        if (isSelected) {
                            btn.classList.add('selected', 'active');
                        }
                        btn.onclick = () => {
                            toggleServerForGroup(serverName);
                        };
                    } else {
                        btn.onclick = null;
                    }

                    const portMatch = (server.url || '').match(/:(\d+)/);
                    const port = portMatch ? portMatch[1] : (server.port || serverName.match(/\d+/)?.[0] || '?');
                    const isSelected = currentGroupCreation && currentGroupCreation.selectedServers.includes(serverName);
                    // 移除状态文字中的“状态: 已选中”
                    const statusText = '状态: 已连接';
                    const botHTML = SERVER_BOT_HTML;

                    btn.innerHTML = botHTML + `
                        <div class="server-button-name" style="position: absolute; bottom: -15px; left: 50%; transform: translateX(-50%); font-size: 11px; color: #2d3436; white-space: nowrap; pointer-events: none; z-index: 100;">${serverName}</div>
                        <div class="server-tooltip">
                            <div style="font-weight: bold; margin-bottom: 4px;">${serverName}</div>
                            <div style="font-size: 11px; opacity: 0.9;">${server.url || ''}</div>
                            <div style="font-size: 11px; color: #00ff88; margin-top: 4px;" class="status-text">${statusText}</div>
                        </div>
                    `;

                    availableGrid.appendChild(btn);
                });

                availableSection.appendChild(availableGrid);
                availableContainer.appendChild(availableSection);
                // 初始化雷达机器人
                initRadarBots();
            }

            const groupsContainer = document.getElementById('userGroupsContainer');
            if (!groupsContainer) return;
            groupsContainer.innerHTML = '';

            if (currentGroupCreation) {
                const createArea = document.createElement('div');
                createArea.className = 'group-creation-area';

                const selectedServerNames = currentGroupCreation.selectedServers;
                const unselectedServers = allServers.filter(s =>
                    !selectedServerNames.includes(s.name) &&
                    (!assignedServers.has(s.name) || selectedServerNames.includes(s.name))
                );

                createArea.innerHTML = `
                    <div class="group-creation-row">
                        <button class="group-select-btn ${currentGroupCreation.userId ? 'selected' : ''}"
                                onclick="selectUserForGroup('${currentGroupCreation.userId || ''}')">
                            ${currentGroupCreation.userId || 'Please select user'}
                        </button>
                        ${!currentGroupCreation.userId ? managerUsers.map(userId => `
                            <button class="group-select-btn"
                                    onclick="selectUserForGroup('${userId}')">
                                ${userId}
                            </button>
                        `).join('') : ''}
                    </div>
                    <div class="group-creation-row">
                        <button class="group-select-btn ${currentGroupCreation.selectedServers.length > 0 ? 'selected' : ''}"
                                onclick="showServerSelection()">
                            ${currentGroupCreation.selectedServers.length > 0
                        ? currentGroupCreation.selectedServers[0]
                        : 'Please select backend server'}
                        </button>
                        ${currentGroupCreation.userId && currentGroupCreation.showingServers ? allServers.filter(s => !assignedServers.has(s.name) || selectedServerNames.includes(s.name)).map(server => {
                            const isSelected = selectedServerNames.includes(server.name);
                            return `<button class="group-select-btn ${isSelected ? 'selected' : ''}"
                                    onclick="toggleServerForGroup('${server.name}')">
                                    ${server.name}
                                </button>`;
                        }).join('') : ''}
                    </div>
                    ${currentGroupCreation.userId && selectedServerNames.length > 0 ? `
                    <div class="group-servers-display">
                        ${selectedServerNames.map(serverName => {
                            const server = allServers.find(s => s.name === serverName);
                            return server ? `
                                <div class="server-tag private">
                                    <div>${server.name}</div>
                                    <div class="server-tag-label">独享服务器</div>
                                </div>
                            ` : '';
                        }).join('')}
                        ${unselectedServers.length > 0 ? '<div style="width: 100%; height: 10px;"></div>' : ''}
                        ${unselectedServers.map(server => `
                            <div class="server-tag public">
                                <div>${server.name}</div>
                                <div class="server-tag-label">共享服务器</div>
                            </div>
                        `).join('')}
                    </div>
                    <div class="group-creation-row">
                        <button class="admin-manage-footer-btn reset" onclick="resetGroupCreation()">重置</button>
                        <button class="admin-manage-footer-btn manage" onclick="confirmGroupCreation()">管理</button>
                    </div>
                    ` : currentGroupCreation.userId && selectedServerNames.length === 0 ? `
                    <div class="group-creation-row">
                        <button class="admin-manage-footer-btn reset" onclick="resetGroupCreation()">重置</button>
                        <button class="admin-manage-footer-btn confirm" onclick="confirmGroupCreation()">确定</button>
                    </div>
                    ` : ''}
                `;
                groupsContainer.appendChild(createArea);
            }

            // 使用API返回的user_groups或本地的managerUserGroups渲染用户组
            const userGroupsToRender = data.user_groups || managerUserGroups;
            userGroupsToRender.forEach(group => {
                const section = document.createElement('div');
                section.className = 'user-group-section';

                const isEditing = currentGroupCreation && currentGroupCreation.userId === group.userId;

                const privateServers = group.servers || [];
                const publicServers = allServers.filter(s => !privateServers.includes(s.name) && !assignedServers.has(s.name));

                section.innerHTML = `
                    <div class="user-group-header">
                        <div class="user-group-name">用户: ${group.userId || group.user_id}</div>
                        <div class="user-group-actions">
                            ${isEditing ? '' : `<button class="admin-account-action-btn manage" onclick="manageUserGroup('${group.userId || group.user_id}')">管理</button>`}
                            <button class="admin-account-action-btn delete" onclick="deleteUserGroup('${group.userId || group.user_id}')">重置</button>
                        </div>
                    </div>
                    <div class="user-group-servers">
                        ${privateServers.map(server => `
                            <div class="server-tag private">
                                <div>${server}</div>
                                <div class="server-tag-label">独享私有服务器</div>
                            </div>
                        `).join('')}
                        ${publicServers.length > 0 ? '<div style="width: 100%; height: 10px;"></div>' : ''}
                        ${publicServers.map(server => `
                            <div class="server-tag public">
                                <div>${server.name}</div>
                                <div class="server-tag-label">公共共享服务器</div>
                            </div>
                        `).join('')}
                    </div>
                `;
                groupsContainer.appendChild(section);
            });
        } catch (error) {
        }
    }, 50);
}



//#endregion
//#region WebSocket连接

window.handleAdminLogin = handleAdminLogin;

/** 是否正在发送 */
let isSending = false;

/** 当前任务ID */
let currentTaskId = null;

/** 任务状态检查定时器 */
let taskStatusCheckTimer = null;

/** 任务状态最后更新时间 */
let taskStatusLastUpdate = null;

/** 任务状态最后进度 */
let taskStatusLastProgress = null;

/** 任务状态最后进度时间 */
let taskStatusLastProgressTime = null;

/** 当前聊天ID */
let currentChatId = null;

/** 未读聊天ID集合 */
window.unreadChatIds = new Set();

/** 新消息通知 */
let newMessageNotification = null;

/** 已清除的聊天ID集合 */
let clearedChatIds = new Set();

/** 全局统计数据 */
let globalStats = {
    taskCount: 0,
    totalSent: 0,
    totalSuccess: 0,
    totalFail: 0,
    totalTime: 0,
    totalPhoneCount: 0,
    inboxReceived: 0,
    inboxSent: 0,
    inboxTotal: 0
};

/** 已发送电话号码集合 */
let sentPhoneNumbers = new Set();

/** 最大日志条目数 */
const MAX_LOG_ITEMS = 200;

/** 日志滚动待处理标志 */
let logScrollPending = false;

/** 会话滚动待处理标志 */
let conversationScrollPending = false;

/** 任务WebSocket等待器映射 */
const _taskWsWaiters = new Map();

/** 确保任务等待器存在 */
function _ensureTaskWaiter(taskId, timeoutMs = 30 * 60 * 1000) {
    if (_taskWsWaiters.has(taskId)) return _taskWsWaiters.get(taskId);
    let resolveFn, rejectFn;
    const p = new Promise((resolve, reject) => {
        resolveFn = resolve;
        rejectFn = reject;
    });
    const timeoutId = setTimeout(() => {
        _taskWsWaiters.delete(taskId);
        try { rejectFn(new Error('WS_TIMEOUT')); } catch { /* ignore */ }
    }, timeoutMs);
    const waiter = { promise: p, resolve: resolveFn, reject: rejectFn, timeoutId };
    _taskWsWaiters.set(taskId, waiter);
    return waiter;
}

/** 连接到后端WebSocket服务 */
function connectToBackendWS(_serverIgnored) {
    if (window.activeWs && (window.activeWs.readyState === WebSocket.OPEN || window.activeWs.readyState === WebSocket.CONNECTING)) {
        return;
    }

    if (!window.authToken) {
        window.authToken = StorageManager.session.getUserToken();
    }
    if (!window.authToken && typeof getAuthToken === 'function') {
        window.authToken = getAuthToken();
    }

    const wsUrl = API_BASE_URL
        .replace('http://', 'ws://')
        .replace('https://', 'wss://')
        .replace('/api', '') + '/ws/frontend';

    try {
        window.activeWs = new WebSocket(wsUrl);

        let heartbeatTimer = null;

        window.activeWs.onopen = () => {
            if (typeof updateConnectionStatus === 'function') {
                updateConnectionStatus(true);
            }

            setTimeout(() => {
                if (window.currentUserId) {
                    sendWSCommand('subscribe_user', { user_id: window.currentUserId });
                }

                if (typeof currentTaskId !== 'undefined' && currentTaskId) {
                    sendWSCommand('subscribe_task', { task_id: currentTaskId });
                }

                if (typeof isSending !== 'undefined' && isSending && typeof currentTaskId !== 'undefined' && currentTaskId) {
                    sendWSCommand('subscribe_task', { task_id: currentTaskId });
                }

                showMessage('已连接到实时推送服务', 'success');
            }, 100);

            if (heartbeatTimer) {
                clearInterval(heartbeatTimer);
            }
            heartbeatTimer = setInterval(() => {
                if (window.activeWs && window.activeWs.readyState === WebSocket.OPEN) {
                    sendWSCommand('ping', {});
                }
            }, 30000);
        };

        window.activeWs.onmessage = (event) => {
            try {
                const msg = JSON.parse(event.data);
                const msgType = msg.type;

                if (msgType === 'task_update') {
                }

                if (msgType === 'task_update') {
                    handleServerMessage(msg, null);
                } else if (msgType === 'balance_update') {
                    handleServerMessage(msg, null);
                } else if (msgType === 'inbox_update') {
                    handleServerMessage(msg, null);
                } else if (msgType === 'initial_chats') {
                    handleServerMessage(msg, null);
                } else if (msgType === 'new_messages') {
                    handleServerMessage(msg, null);
                } else if (msgType === 'conversation_data') {
                    handleServerMessage(msg, null);
                } else if (msgType === 'subscribed') {
                } else if (msgType === 'user_subscribed') {
                } else if (msgType === 'unsubscribed') {
                } else if (msgType === 'pong') {
                } else if (msgType === 'error') {
                } else if (msgType === 'super_admin_response') {
                    handleSuperAdminResponse(msg);
                } else if (msgType === 'servers_list' || msgType === 'servers_list_update' || msgType === 'server_update') {
                    if ((msgType === 'servers_list' || msgType === 'servers_list_update') && msg.servers) {
                        serverData.connected = [];
                        serverData.disconnected = [];
                        if (Array.isArray(msg.servers)) {
                            msg.servers.forEach(server => {
                                const serverItem = {
                                    name: server.server_name || server.server_id,
                                    url: server.server_url || '',
                                    server_id: server.server_id,
                                    status: (server.status || '').toLowerCase(),
                                    assigned_user_id: server.assigned_user_id || null,
                                    last_seen: server.last_seen
                                };

                                const isOnline =
                                    serverItem.status === 'connected' ||
                                    serverItem.status === 'available' ||
                                    serverItem.status === 'ready' ||
                                    serverItem.status === 'online';

                                if (isOnline) {
                                    serverItem.status = 'connected';
                                    serverData.connected.push(serverItem);
                                } else {
                                    serverItem.status = 'disconnected';
                                    serverData.disconnected.push(serverItem);
                                }
                            });
                        }
                        if (typeof updateServerDisplay === 'function') {
                            updateServerDisplay();
                        }
                        if (typeof connectToAssignedServers === 'function') {
                            connectToAssignedServers();
                        }
                        if (typeof connectToAvailableServers === 'function') {
                            connectToAvailableServers();
                        }
                    } else if (msgType === 'server_update') {
                        if (typeof loadServersFromAPI === 'function') {
                            loadServersFromAPI();
                        }
                    }
                }
            } catch (e) {
            }
        };

        window.activeWs.onerror = (error) => {
            if (typeof updateConnectionStatus === 'function') {
                updateConnectionStatus(false);
            }
            try { showMessage('实时推送连接失败（WS）', 'warning'); } catch { }
        };

        window.activeWs.onclose = (event) => {
            if (typeof updateConnectionStatus === 'function') {
                updateConnectionStatus(false);
            }

            if (heartbeatTimer) {
                clearInterval(heartbeatTimer);
                heartbeatTimer = null;
            }

            if (typeof stopOnlineServersTimer === 'function') {
                stopOnlineServersTimer();
            }

            window.activeWs = null;

            setTimeout(() => {
                if (!window.activeWs) {
                    connectToBackendWS(_serverIgnored);
                }
            }, 5000);
        };

    } catch (e) {
        window.activeWs = null;
        if (typeof updateConnectionStatus === 'function') {
            updateConnectionStatus(false);
        }
        showMessage('WebSocket 初始化失败', 'error');
    }
}

/** 发送WebSocket命令 */
function sendWSCommand(action, data = {}) {
    if (!window.activeWs || window.activeWs.readyState !== WebSocket.OPEN) {
        return false;
    }
    const payload = JSON.stringify({ action, data });
    try {
        window.activeWs.send(payload);
        return true;
    } catch (e) {
        return false;
    }
}

/** 更新用户信息显示 */
function updateUserInfoDisplay(credits) {
    const userInfoDisplay = document.getElementById('userInfoDisplay');
    const currentCreditsEl = document.getElementById('currentCredits');

    if (userInfoDisplay && currentCreditsEl) {
        if (credits !== undefined && credits !== null) {
            currentCreditsEl.dataset.raw = credits;
            if (typeof formatCurrencyDisplay === 'function') {
                currentCreditsEl.textContent = formatCurrencyDisplay(credits);
            } else {
                currentCreditsEl.textContent = typeof credits === 'number' ? credits.toFixed(2) : credits;
            }
        } else {
            currentCreditsEl.textContent = '-';
        }
        if (typeof updateCreditLabel === 'function') {
            updateCreditLabel();
        }
        userInfoDisplay.style.display = 'inline-block';
    }
}

/** 加载用户后端列表 */
async function loadUserBackends() {
    const userId = window.currentUserId;
    const token = window.authToken;
    
    if (!userId) {
        window.currentUserId = StorageManager.session.getUserId();
    }
    if (!token) {
        window.authToken = checkAuthToken();
    }
    
    if (!window.currentUserId || !window.authToken) {
        return;
    }

    try {
        const response = await fetch(`${API_BASE_URL}/user/${window.currentUserId}/backends`, {
            headers: {
                'Authorization': `Bearer ${window.authToken}`
            }
        });

        if (response.ok) {
            const data = await response.json();
            const backends = data.backends || data.backend_servers || [];
            StorageManager.user.setUserBackends(backends);
            return backends;
        }
    } catch (error) {
    }
    return [];
}

/** 检查用户认证状态 */
async function checkAuth() {
    const SESSION_TIMEOUT = 60 * 60 * 1000;
    const loginTime = StorageManager.session.getLoginTime();
    
    if (loginTime) {
        const timeSinceLogin = Date.now() - loginTime;
        if (timeSinceLogin > SESSION_TIMEOUT) {
            StorageManager.session.clearLoginTime();
            return false;
        }
    }
    
    window.currentUserId = StorageManager.session.getUserId();
    window.authToken = StorageManager.session.getUserToken();
    if (!window.currentUserId || !window.authToken) {
        return false;
    }
    
    return true;
}

/** 获取字符串长度（中文算2个字符） */
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

/** 更新号码和消息计数 */
function updateCounts() {
    const numbersText = document.getElementById('numbersText').value;
    const numbers = numbersText.split(/[\n,]/).filter(n => n.trim()).length;
    const numbersCountEl = document.getElementById('numbersCount');

    if (numbers === 0) {
        numbersCountEl.textContent = `号码: ${numbers}`;
        numbersCountEl.classList.remove('has-numbers');
    } else {
        numbersCountEl.textContent = `号码: ${numbers}`;
        numbersCountEl.classList.add('has-numbers');
    }

    const messageText = document.getElementById('messageText').value;
    const charCount = getStringLength(messageText);
    const messageCountEl = document.getElementById('messageCount');

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

/** 导入号码文件 */
function importNumbers() {
    document.getElementById('numbersFile').click();
}

/** 导入消息文件 */
function importMessage() {
    document.getElementById('messageFile').click();
}

/** 清空号码输入框 */
function clearNumbers() {
    const btn = document.getElementById('clearNumbersBtn');
    document.getElementById('numbersText').value = '';
    updateCounts();
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

/** 清空消息输入框 */
function clearMessage() {
    const btn = document.getElementById('clearMessageBtn');
    document.getElementById('messageText').value = '';
    updateCounts();
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

document.getElementById('numbersFile').addEventListener('change', function (e) {
    const file = e.target.files[0];
    if (file) {
        const reader = new FileReader();
        reader.onload = function (e) {
            const content = e.target.result;
            const numbers = content.split(/[\n,]/)
                .map(n => n.trim())
                .filter(n => n.length > 0);
            document.getElementById('numbersText').value = numbers.join('\n');
            updateCounts();
        };
        reader.readAsText(file);
    }
    this.value = '';
});

document.getElementById('messageFile').addEventListener('change', function (e) {
    const file = e.target.files[0];
    if (file) {
        const reader = new FileReader();
        reader.onload = function (e) {
            document.getElementById('messageText').value = e.target.result;
            updateCounts();
        };
        reader.readAsText(file);
    }
    this.value = '';
});

/** 更新连接状态显示 */
function updateConnectionStatus(connected) {
    const statusEl = document.getElementById('connectionStatus');
    if (!statusEl) return;

    if (connected) {
        statusEl.innerHTML = '<span style="color: white; font-weight: bold;">●</span> 已连接';
        statusEl.className = 'connection-status status-connected';
    } else {
        statusEl.innerHTML = '<span style="color: white; font-weight: bold;">●</span> 未连接';
        statusEl.className = 'connection-status status-disconnected';
    }
}

/** 是否正在连接服务器 */
let isConnectingServers = false;

/** 是否已警告无可用服务器 */
let _noUsableServerWarned = false;

/** 连接到可用服务器 */
async function connectToAvailableServers() {
    if (!checkAuth()) return;

    if (isConnectingServers) return;
    isConnectingServers = true;

    try {
        const hasConnectedServers = serverData.connected && serverData.connected.length > 0;

        if (!hasConnectedServers) {
            if (!_serversLoadedOnce) {
                let waitCount = 0;
                const maxWait = 30;
                while (!_serversLoadedOnce && waitCount < maxWait) {
                    await new Promise(resolve => setTimeout(resolve, 100));
                    waitCount++;
                }
                if (!_serversLoadedOnce) {
                    return;
                }
                const hasServersNow = serverData.connected && serverData.connected.length > 0;
                if (!hasServersNow) {
                    updateConnectionStatus(false);
                    if (!_noUsableServerWarned) {
                        _noUsableServerWarned = true;
                    }
                    return;
                }
            } else {
                updateConnectionStatus(false);
                if (!_noUsableServerWarned) {
                    _noUsableServerWarned = true;
                }
                return;
            }
        }

        updateConnectionStatus(true);
        _noUsableServerWarned = false;
    } finally {
        isConnectingServers = false;
    }
}

//#endregion
//#region 文件导入事件监听

document.getElementById('numbersFile').addEventListener('change', function (e) {
    const file = e.target.files[0];
    if (file) {
        const reader = new FileReader();
        reader.onload = function (e) {
            const content = e.target.result;
            const numbers = content.split(/[\n,]/)
                .map(n => n.trim())
                .filter(n => n.length > 0);
            document.getElementById('numbersText').value = numbers.join('\n');
            updateCounts();
        };
        reader.readAsText(file);
    }
    this.value = '';
});

document.getElementById('messageFile').addEventListener('change', function (e) {
    const file = e.target.files[0];
    if (file) {
        const reader = new FileReader();
        reader.onload = function (e) {
            document.getElementById('messageText').value = e.target.result;
            updateCounts();
        };
        reader.readAsText(file);
    }
    this.value = '';
});

//#endregion
