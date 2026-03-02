//#region 费率与货币转换逻辑
// displayMode: 'credit' = 显示积分, 'count' = 显示可发送数量
let displayMode = 'credit';

/** 格式化余额显示(积分或可发送数量) */
function formatCurrencyDisplay(credits) {
    const num = parseFloat(credits);
    if (isNaN(num)) return '-';
    if (displayMode === 'count') {
        const sendRate = window.currentUserSendRate || 30;
        const count = Math.floor(num / sendRate);
        return String(count);
    }
    return num.toFixed(2);
}

/** 切换显示模式(积分/可发送数量) */
function toggleCurrencyMode() {
    displayMode = displayMode === 'credit' ? 'count' : 'credit';
    updateCurrencyUI();
    updateDisplayedBalances();
    updateCreditLabel();
    const label = displayMode === 'credit' ? '积分' : '可发数量';
    showAutoToast(`已切换为: ${label}`, 'info');
}

/** 更新UI切换按钮状态 */
function updateCurrencyUI() {
    const toggle = document.getElementById('currencyToggle');
    if (toggle) {
        if (displayMode === 'count') {
            toggle.classList.add('count');
            toggle.classList.remove('credit');
        } else {
            toggle.classList.add('credit');
            toggle.classList.remove('count');
        }
    }
}

/** 更新标签文字 */
function updateCreditLabel() {
    const labelEl = document.getElementById('creditLabel');
    if (labelEl) {
        labelEl.textContent = displayMode === 'credit' ? '剩余积分:' : '可发送:';
    }
}

/** 更新所有显示的余额数值 */
function updateDisplayedBalances() {
    const balanceEl = document.getElementById('currentCredits');
    if (balanceEl && balanceEl.dataset.raw) {
        balanceEl.textContent = formatCurrencyDisplay(balanceEl.dataset.raw);
    }
    const saCreditsEl = document.getElementById('saRechargeInfoCredits');
    if (saCreditsEl && saCreditsEl.dataset.raw) {
        saCreditsEl.textContent = formatCurrencyDisplay(saCreditsEl.dataset.raw);
    }
    updateCreditLabel();
}

/** 保存全局费率设置 */
function saSaveGlobalRate() {
    const usdVal = document.getElementById('saGlobalRateUSD').value;
    if (usdVal) {
        globalExchangeRate = parseFloat(usdVal);
        StorageManager.preferences.setGlobalExchangeRate(globalExchangeRate);
        showAutoToast('全局费率已生效 (本地记录)', 'success');
        updateDisplayedBalances();
    }
}

/** 保存业务员调价范围 */
function saSaveSalesRateRange() {
    const min = document.getElementById('saSalesMinRate').value;
    const max = document.getElementById('saSalesMaxRate').value;
    showAutoToast(`业务员调价范围已设置: ${min} ~ ${max}`, 'success');
}

/** 查询用户费率并加载配置面板 */
function saQueryUserRate() {
    const username = document.getElementById('saRateTargetUser').value;
    if (!username) return showAutoToast('请输入用户名', 'warning');
    document.getElementById('saUserRateControl').style.display = 'block';
    document.getElementById('saUserRatePlaceholder').style.display = 'none';
    document.getElementById('saUserCustomRate').value = globalExchangeRate;
    showAutoToast(`已加载用户 ${username} 的当前配置`, 'info');
}

/** 保存用户自定义费率 */
function saSaveUserRate() {
    const username = document.getElementById('saRateTargetUser').value;
    const rate = document.getElementById('saUserCustomRate').value;
    showAutoToast(`用户 ${username} 的专属费率 ${rate} 已应用`, 'success');
}

document.addEventListener('DOMContentLoaded', () => {
    const usdInput = document.getElementById('saGlobalRateUSD');
    const creditInput = document.getElementById('saGlobalRateCredit');
    if (usdInput && creditInput) {
        usdInput.value = globalExchangeRate;
        creditInput.value = (1 / globalExchangeRate).toFixed(1);
        usdInput.addEventListener('input', () => {
            const val = parseFloat(usdInput.value);
            if (val > 0) creditInput.value = (1 / val).toFixed(1);
        });
        creditInput.addEventListener('input', () => {
            const val = parseFloat(creditInput.value);
            if (val > 0) usdInput.value = (1 / val).toFixed(5);
        });
    }
    updateCurrencyUI();
    updateCreditLabel();
});
//#endregion

//#region API配置
let API_BASE_URL;
(function initApiBase() {
    function normalizeApiBase(raw) {
        let v = (raw || '').trim();
        if (!v) return null;
        if (!/^https?:\/\//i.test(v)) {
            const isLocalhost = /^(localhost|127\.|192\.168\.|10\.|172\.(1[6-9]|2[0-9]|3[01])\.)/i.test(v.split(':')[0]);
            const proto = isLocalhost ? 'http://' : 'https://';
            v = proto + v;
        }
        v = v.replace(/\/+$/, '');
        if (!/\/api$/i.test(v)) v = v + '/api';
        return v;
    }
    function setApiBaseInternal(raw) {
        const norm = normalizeApiBase(raw);
        return norm;
    }
    window.setApiBase = function setApiBase(v) {
        const norm = setApiBaseInternal(v);
        if (norm) {
            API_BASE_URL = norm;
            // 不保存到localStorage，仅当前会话有效
            location.reload();
        }
    };
    const params = new URLSearchParams(location.search || '');
    const fromQuery = (params.get('api') || '').trim();
    // 不从localStorage读取，只从查询参数或自动推断
    if (fromQuery) {
        const norm = setApiBaseInternal(fromQuery);
        if (norm) {
            API_BASE_URL = norm;
            return;
        }
    }
    if (location.protocol === 'file:') {
        API_BASE_URL = 'http://localhost:28080/api';
        return;
    }
    if (location.hostname && location.hostname !== 'localhost' && location.hostname !== '127.0.0.1') {
        API_BASE_URL = location.origin + '/api';
        return;
    }
    API_BASE_URL = 'http://localhost:28080/api';
})();

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
//#endregion

//#region 全局变量声明
window.currentUserId = null;
window.authToken = null;
window.activeWs = null;
window.activeWsServer = null;
//#endregion

//#region 用户登录表单
/** 切换到用户登录模式 */
function switchToUser() {
    const loginPanel = document.getElementById('loginPanel');
    const adminToggle = document.getElementById('adminToggle');
    document.getElementById('userLoginForm').style.display = 'block';
    document.getElementById('adminLoginForm').style.display = 'none';
    document.getElementById('registerForm').style.display = 'none';
    loginPanel.classList.remove('admin-mode');
    adminToggle.classList.remove('active');
    document.querySelector('.login-logo').textContent = '用户登录';
    if (adminToggle) {
        adminToggle.textContent = 'Admin';
    }
    clearMessage();
}

/** 切换到管理员登录模式 */
window.switchToAdmin = function switchToAdmin() {
    const loginPanel = document.getElementById('loginPanel');
    const adminToggle = document.getElementById('adminToggle');
    const isAdminMode = loginPanel.classList.contains('admin-mode');
    if (isAdminMode) {
        document.getElementById('userLoginForm').style.display = 'block';
        document.getElementById('adminLoginForm').style.display = 'none';
        document.getElementById('registerForm').style.display = 'none';
        adminToggle.classList.remove('active');
        loginPanel.classList.remove('admin-mode');
        document.querySelector('.login-logo').textContent = '用户登录';
        adminToggle.textContent = 'AGENT';
    } else {
        document.getElementById('userLoginForm').style.display = 'none';
        document.getElementById('adminLoginForm').style.display = 'block';
        document.getElementById('registerForm').style.display = 'none';
        adminToggle.classList.add('active');
        loginPanel.classList.add('admin-mode');
        document.querySelector('.login-logo').textContent = 'Agent Login';
        adminToggle.textContent = 'User';
    }
    clearMessage();
}

/** 显示用户注册表单 */
function showRegister() {
    document.getElementById('userLoginForm').style.display = 'none';
    document.getElementById('adminLoginForm').style.display = 'none';
    document.getElementById('registerForm').style.display = 'block';
    const logo = document.querySelector('.login-logo');
    if (logo) logo.textContent = '用户注册';
    clearMessage();
}

/** 显示用户登录表单 */
function showLogin() {
    document.getElementById('registerForm').style.display = 'none';
    document.getElementById('adminLoginForm').style.display = 'none';
    document.getElementById('userLoginForm').style.display = 'block';
    const logo = document.querySelector('.login-logo');
    if (logo) logo.textContent = '用户登录';
    clearMessage();
}

/** 清空登录消息提示 */
function clearMessage() {
    const msg = document.getElementById('authMessage');
    msg.className = 'message-box';
    msg.textContent = '';
}

/** 显示自动消失的提示消息 */
function showAutoToast(message, type = 'success') {
    const toast = document.createElement('div');
    toast.className = `auto-toast auto-toast-${type}`;
    toast.textContent = message;
    document.body.appendChild(toast);
    requestAnimationFrame(() => {
        toast.classList.add('show');
    });
    setTimeout(() => {
        toast.classList.remove('show');
        setTimeout(() => {
            document.body.removeChild(toast);
        }, 300);
    }, 3000);
}

/** 显示登录消息(兼容旧API) */
function showMessage(text, type) {
    if (type === 'error') {
        showAutoToast(text, type);
    } else {
        showAutoToast(text, type);
    }
}

/** 处理用户登录请求 */
async function handleLogin() {
    const usernameEl = document.getElementById('loginUsername');
    const passwordEl = document.getElementById('loginPassword');
    if (!usernameEl || !passwordEl) {
        return;
    }
    const username = usernameEl.value.trim();
    const password = passwordEl.value.trim();
    if (!username || !password) {
        if (typeof showMessage === 'function') {
            showMessage('请输入用户名和密码', 'error');
        } else {
            await customAlert('请输入用户名和密码');
        }
        return;
    }
    try {
        const response = await fetch(`${API_BASE_URL}/login`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ username, password })
        });
        const data = await response.json();
        if (response.ok && (data.ok || data.success)) {
            window.currentUserId = data.user_id;
            window.authToken = data.token;
            window.currentUsername = data.username || username;
            window.currentUserSendRate = data.send_rate || 30;
            StorageManager.session.setUserToken(data.token);
            StorageManager.session.setUserId(data.user_id);
            StorageManager.session.setUsername(data.username || username);
            StorageManager.session.setLoginTime(Date.now());
            
            showMessage('登录成功！正在跳转...', 'success');
            setTimeout(() => {
                const loginPage = document.getElementById('loginPage');
                const contentWrapper = document.querySelector('.content-wrapper');
                const mainContainer = document.querySelector('.main-container');
                if (loginPage) loginPage.style.display = 'none';
                document.body.classList.remove('login-mode');
                if (contentWrapper) contentWrapper.style.display = 'flex';
                if (mainContainer) mainContainer.style.display = 'flex';
                const userBalance = data.balance !== undefined ? data.balance : data.credits;
                if (userBalance !== undefined && typeof updateUserInfoDisplay === 'function') {
                    updateUserInfoDisplay(userBalance);
                }
                if (typeof showMainApp === 'function') showMainApp();
                if (typeof window.init === 'function') window.init();
                if (typeof switchPanel === 'function') {
                    setTimeout(() => switchPanel('home'), 200);
                }
            }, 500);
        } else {
            const errorMsg = data.message || '密码错误';
            if (typeof showMessage === 'function') {
                showMessage(errorMsg, 'error');
            } else {
                await customAlert(errorMsg);
            }
        }
    } catch (error) {
        let errorMsg = '登录失败';
        try {
            if (error.response) {
                const errorData = await error.response.json();
                errorMsg = errorData.message || '密码错误';
            } else if (error.message && error.message.includes('fetch')) {
                errorMsg = '网络连接失败，请稍后重试';
            }
        } catch (e) {
            errorMsg = '密码错误';
        }
        if (typeof showMessage === 'function') {
            showMessage(errorMsg, 'error');
        } else {
            await customAlert(errorMsg);
        }
    }
}
window.handleLogin = handleLogin;

//#endregion

//#region 管理员登录表单
/** 处理管理员登录请求 */
async function handleAdminLogin() {
    const username = document.getElementById('adminLoginUsername').value.trim();
    const password = document.getElementById('adminLoginPassword').value.trim();
    if (!username || !password) {
        showMessage('请输入管理员用户名和密码', 'error');
        return;
    }
    async function doAdminLogin() {
        try {
            if (typeof showMessage === 'function') {
                showMessage('正在验证管理员身份...', 'info');
            }
            const response = await fetch(`${API_BASE_URL}/admin/login`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ admin_id: username, password: password })
            });
            const data = await response.json().catch(() => ({}));
            if (response.ok && (data.ok || data.success) && data.token) {
                StorageManager.session.setAdminToken(data.token);
                showMessage('管理员登录成功！正在跳转...', 'success');
                setTimeout(() => loginAsManager(username), 500);
                return;
            }
            const errorMsg = data.message || '管理员登录失败，请检查用户名和密码';
            if (typeof showMessage === 'function') showMessage(errorMsg, 'error');
            else await customAlert(errorMsg);
        } catch (error) {
            const errorMsg = (error && error.message && error.message.includes('fetch')) ? '网络连接失败，请稍后重试' : '管理员登录失败';
            if (typeof showMessage === 'function') showMessage(errorMsg, 'error');
            else await customAlert(errorMsg);
        }
    }
    await doAdminLogin();
}

//#endregion

//#region 用户注册表单
/** 处理用户注册请求 */
async function handleRegister() {
    const usernameEl = document.getElementById('registerUsername');
    const passwordEl = document.getElementById('registerPassword');
    const confirmPasswordEl = document.getElementById('registerConfirmPassword');
    if (!usernameEl || !passwordEl || !confirmPasswordEl) {
        return;
    }
    const username = usernameEl.value.trim();
    const password = passwordEl.value.trim();
    const confirmPassword = confirmPasswordEl.value.trim();
    if (!username) {
        if (typeof showMessage === 'function') {
            showMessage('请输入用户名', 'error');
        } else {
            await customAlert('请输入用户名');
        }
        return;
    }
    if (username.length < 4) {
        if (typeof showMessage === 'function') {
            showMessage('用户名至少需要4位', 'error');
        } else {
            await customAlert('用户名至少需要4位');
        }
        return;
    }
    if (!/^[a-zA-Z0-9]+$/.test(username)) {
        if (typeof showMessage === 'function') {
            showMessage('用户名只能包含字母或数字', 'error');
        } else {
            await customAlert('用户名只能包含字母或数字');
        }
        return;
    }
    if (!password) {
        if (typeof showMessage === 'function') {
            showMessage('请输入密码', 'error');
        } else {
            await customAlert('请输入密码');
        }
        return;
    }
    if (password.length < 4) {
        if (typeof showMessage === 'function') {
            showMessage('密码至少需要4位', 'error');
        } else {
            await customAlert('密码至少需要4位');
        }
        return;
    }
    if (!confirmPassword) {
        if (typeof showMessage === 'function') {
            showMessage('请确认密码', 'error');
        } else {
            await customAlert('请确认密码');
        }
        return;
    }
    if (password !== confirmPassword) {
        if (typeof showMessage === 'function') {
            showMessage('两次输入的密码不一致', 'error');
        } else {
            await customAlert('两次输入的密码不一致');
        }
        return;
    }
    async function doRegister() {
        try {
            if (typeof showMessage === 'function') {
                showMessage('正在注册...', 'info');
            }
            const response = await fetch(`${API_BASE_URL}/register`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ username: username, password: password })
            });
            const data = await response.json();
            if (response.ok && (data.ok || data.success)) {
                document.getElementById('registerUsername').value = '';
                document.getElementById('registerPassword').value = '';
                document.getElementById('registerConfirmPassword').value = '';
                if (typeof showLogin === 'function') {
                    showLogin();
                }
                const loginUsernameEl = document.getElementById('loginUsername');
                const loginPasswordEl = document.getElementById('loginPassword');
                if (loginUsernameEl) {
                    loginUsernameEl.value = username;
                }
                if (loginPasswordEl) {
                    loginPasswordEl.value = '';
                }
                setTimeout(async () => {
                    await customAlert('注册成功！');
                }, 300);
            } else {
                const errorMsg = data.message || '注册失败';
                if (typeof showMessage === 'function') {
                    showMessage(errorMsg, 'error');
                } else {
                    await customAlert(errorMsg);
                }
            }
        } catch (error) {
            let errorMsg = '注册失败';
            if (error.message && error.message.includes('fetch')) {
                errorMsg = '网络连接失败，请稍后重试';
            } else {
                try {
                    if (error.response) {
                        const errorData = await error.response.json();
                        errorMsg = errorData.message || '注册失败';
                    }
                } catch (e) {
                    errorMsg = '注册失败';
                }
            }
            if (typeof showMessage === 'function') {
                showMessage(errorMsg, 'error');
            } else {
                await customAlert(errorMsg);
            }
        }
    }
    doRegister();
}

/** 切换密码显示/隐藏 */
function togglePassword(inputId, button) {
    const input = document.getElementById(inputId);
    if (input.type === 'password') {
        input.type = 'text';
        button.textContent = '🙈';
    } else {
        input.type = 'password';
        button.textContent = '👁️';
    }
}

/** 处理忘记密码(提示联系管理员) */
function handleForgotPassword() {
    customAlert('请联系管理员并提供正确的用户名');
}
//#endregion

//#region 服务器管理员登录
/** 显示服务器管理员登录弹窗 */
window.showAdminModal = function showAdminModal() {
    const modal = document.getElementById('adminModal');
    if (!modal) return;
    modal.style.display = 'flex';
    requestAnimationFrame(() => {
        modal.classList.add('show');
        setTimeout(() => {
            document.getElementById('adminPasswordInput').focus();
        }, 50);
    });
}

/** 关闭服务器管理员登录弹窗 */
function closeAdminModal() {
    const modal = document.getElementById('adminModal');
    if (!modal) return;
    modal.classList.remove('show');
    setTimeout(() => {
        modal.style.display = 'none';
        document.getElementById('adminPasswordInput').value = '';
        document.getElementById('adminMessage').className = 'modal-message';
    }, 300);
}

/** 验证服务器管理员密码 */
async function verifyAdminPassword() {
    const password = (document.getElementById('adminPasswordInput').value || '').trim();
    const msg = document.getElementById('adminMessage');
    if (!password) {
        msg.className = 'modal-message error';
        msg.textContent = '请输入密码';
        setTimeout(() => {
            msg.className = 'modal-message';
            msg.textContent = '';
        }, 3000);
        return;
    }
    try {
        const response = await fetch(`${API_BASE_URL}/server-manager/login`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ password: password, admin_password: password })
        });
        const data = await response.json().catch(() => ({}));
        if (!response.ok || !data.success || !data.token) {
            const errorMsg = data.message || '密码错误';
            msg.className = 'modal-message error';
            msg.textContent = errorMsg;
            setTimeout(() => {
                msg.className = 'modal-message';
                msg.textContent = '';
            }, 5000);
            return;
        }
        if (data.success) {
            StorageManager.session.setServerManagerToken(data.token);
            closeAdminModal();
            const loginPage = document.getElementById('loginPage');
            const serverManagerPage = document.getElementById('adminPage');
            if (loginPage) {
                loginPage.style.display = 'none';
                document.body.classList.remove('login-mode');
            }
            if (serverManagerPage) {
                serverManagerPage.style.display = 'block';
                setTimeout(async () => { try { await loadServersFromAPI(); } catch {} try { updateServerDisplay(); } catch {} }, 100);
                setTimeout(async () => { try { await loadAdminAccountsFromAPI(); } catch {} try { updateAdminAccountDisplay(); } catch {} }, 200);
            }
        } else {
            msg.className = 'modal-message error';
            msg.textContent = data.message || '密码错误，请重试';
            setTimeout(() => {
                msg.className = 'modal-message';
                msg.textContent = '';
            }, 5000);
        }
    } catch (error) {
        let errorMsg = '密码错误';
        try {
            if (error.response) {
                const errorData = await error.response.json();
                errorMsg = errorData.message || '密码错误';
            } else if (error.message && error.message.includes('fetch')) {
                errorMsg = '网络连接失败，请稍后重试';
            }
        } catch (e) {
            errorMsg = '密码错误';
        }
        msg.className = 'modal-message error';
        msg.textContent = errorMsg;
        setTimeout(() => {
            msg.className = 'modal-message';
            msg.textContent = '';
        }, 5000);
    }
}

/** 从服务器管理页返回(选择登录界面或主面板) */
async function backToLogin() {
    const result = await showCustomModal('配置已保存','', 'alert', '', [
        { text: '返回登录界面', value: 'login' },
        { text: '进入主面板', value: 'main' }
    ]);
    if (result === 'login') {
        const serverManagerPage = document.getElementById('adminPage');
        const adminPage = document.getElementById('adminPage');
        const loginPage = document.getElementById('loginPage');
        if (serverManagerPage) { serverManagerPage.style.display = 'none'; }
        if (adminPage) { adminPage.classList.remove('show'); adminPage.style.display = 'none'; }
        if (loginPage) { loginPage.style.display = 'flex'; document.body.classList.add('login-mode'); }
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
        if (typeof stopOnlineServersTimer === 'function') { stopOnlineServersTimer(); }
    } else if (result === 'main') {
        const serverManagerPage = document.getElementById('adminPage');
        const adminPage = document.getElementById('adminPage');
        const loginPage = document.getElementById('loginPage');
        if (serverManagerPage) { serverManagerPage.style.display = 'none'; }
        if (adminPage) { adminPage.classList.remove('show'); adminPage.style.display = 'none'; }
        if (loginPage) { loginPage.style.display = 'none'; document.body.classList.remove('login-mode'); }
        const contentWrapper = document.querySelector('.content-wrapper');
        const mainContainer = document.querySelector('.main-container');
        if (contentWrapper) { contentWrapper.style.display = 'flex'; }
        if (mainContainer) { mainContainer.style.display = 'flex'; }
        const navHomeBtn = document.getElementById('navHomeBtn');
        if (navHomeBtn && typeof navHomeBtn.click === 'function') { navHomeBtn.click(); }
        if (typeof stopOnlineServersTimer === 'function') { stopOnlineServersTimer(); }
    }
}
//#endregion

//#region 通用弹窗组件
let customModalResolve = null;

/** 显示通用弹窗(支持alert/confirm/prompt/自定义按钮) */
function showCustomModal(title, message, type = 'alert', defaultValue = '', customButtons = null, customClass = '') {
    const modal = document.getElementById('customModal');
    const panel = document.getElementById('customModalPanel');
    const titleEl = document.getElementById('customModalTitle');
    const messageEl = document.getElementById('customModalMessage');
    const inputEl = document.getElementById('customModalInput');
    const buttonsEl = document.getElementById('customModalButtons');
    panel.className = 'custom-modal-panel';
    if (customClass) { panel.className += ' ' + customClass; }
    titleEl.textContent = title;
    if (typeof message === 'string') { messageEl.textContent = message; } else { messageEl.innerHTML = message; }
    if (type === 'prompt') {
        inputEl.style.display = 'block';
        inputEl.value = defaultValue;
        inputEl.focus();
    } else {
        inputEl.style.display = 'none';
    }
    buttonsEl.innerHTML = '';
    if (customButtons && Array.isArray(customButtons)) {
        customButtons.forEach(btnConfig => {
            const btn = document.createElement('button');
            btn.className = 'custom-modal-btn confirm';
            btn.textContent = btnConfig.text;
            btn.onclick = () => closeCustomModal(btnConfig.value);
            buttonsEl.appendChild(btn);
        });
    } else if (type === 'alert') {
        const btn = document.createElement('button');
        btn.className = 'custom-modal-btn confirm';
        btn.textContent = '确定';
        btn.onclick = () => closeCustomModal(true);
        buttonsEl.appendChild(btn);
    } else if (type === 'confirm') {
        const cancelBtn = document.createElement('button');
        cancelBtn.className = 'custom-modal-btn cancel';
        cancelBtn.textContent = '取消';
        cancelBtn.onclick = () => closeCustomModal(false);
        buttonsEl.appendChild(cancelBtn);
        const confirmBtn = document.createElement('button');
        confirmBtn.className = 'custom-modal-btn confirm';
        confirmBtn.textContent = '确定';
        confirmBtn.onclick = () => closeCustomModal(true);
        buttonsEl.appendChild(confirmBtn);
    } else if (type === 'prompt') {
        const cancelBtn = document.createElement('button');
        cancelBtn.className = 'custom-modal-btn cancel';
        cancelBtn.textContent = '取消';
        cancelBtn.onclick = () => closeCustomModal(null);
        buttonsEl.appendChild(cancelBtn);
        const confirmBtn = document.createElement('button');
        confirmBtn.className = 'custom-modal-btn confirm';
        confirmBtn.textContent = '确定';
        confirmBtn.onclick = () => {
            const value = inputEl.value.trim();
            closeCustomModal(value || null);
        };
        buttonsEl.appendChild(confirmBtn);
    }
    requestAnimationFrame(() => {
        modal.classList.add('show');
    });
    const handleEnter = (e) => {
        if (e.key === 'Enter') {
            if (type === 'prompt') {
                const value = inputEl.value.trim();
                closeCustomModal(value || null);
            } else {
                closeCustomModal(true);
            }
            inputEl.removeEventListener('keypress', handleEnter);
        }
    };
    if (type === 'prompt') {
        inputEl.addEventListener('keypress', handleEnter);
    }
    return new Promise((resolve) => {
        customModalResolve = resolve;
    });
}

/** 关闭通用弹窗并返回结果 */
function closeCustomModal(result) {
    const modal = document.getElementById('customModal');
    const panel = document.getElementById('customModalPanel');
    const content = document.getElementById('customModalContent');
    if (!modal) {
        if (customModalResolve) {
            customModalResolve(result);
            customModalResolve = null;
        }
        return;
    }
    modal.classList.remove('show');
    setTimeout(() => {
        if (panel) panel.className = 'custom-modal-panel';
        if (content) content.className = 'custom-modal-content';
        if (customModalResolve) {
            customModalResolve(result);
            customModalResolve = null;
        }
    }, 300);
}

/** 简化版提示弹窗 */
async function customAlert(message) {
    await showCustomModal('提示', message, 'alert');
}

/** 简化版确认弹窗 */
async function customConfirm(message, customClass = '') {
    return await showCustomModal('确认', message, 'confirm', '', null, customClass);
}

/** 简化版输入弹窗 */
async function customPrompt(message, defaultValue = '') {
    return await showCustomModal('输入', message, 'prompt', defaultValue);
}
//#endregion

//#region 事件绑定
document.getElementById('adminPasswordInput').addEventListener('keypress', function (e) {
    if (e.key === 'Enter') { verifyAdminPassword(); }
});
document.getElementById('loginPassword').addEventListener('keypress', function (e) {
    if (e.key === 'Enter') { handleLogin(); }
});
document.getElementById('adminLoginPassword').addEventListener('keypress', function (e) {
    if (e.key === 'Enter') { handleAdminLogin(); }
});
document.getElementById('registerUsername').addEventListener('keypress', function (e) {
    if (e.key === 'Enter') { document.getElementById('registerPassword').focus(); }
});
document.getElementById('registerPassword').addEventListener('keypress', function (e) {
    if (e.key === 'Enter') { document.getElementById('registerConfirmPassword').focus(); }
});
document.getElementById('registerConfirmPassword').addEventListener('keypress', function (e) {
    if (e.key === 'Enter') { handleRegister(); }
});
//#endregion
