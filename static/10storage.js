

(function() {
    'use strict';
    
    const STORAGE_VERSION = 'v3';
    const VERSION_KEY = 'storage_ver';
    
    const saved = sessionStorage.getItem(VERSION_KEY);
    if (saved !== STORAGE_VERSION) {
        sessionStorage.clear();
        sessionStorage.setItem(VERSION_KEY, STORAGE_VERSION);
    }
    
    const TOKEN_KEYS = {
        USER: ['utk_' + STORAGE_VERSION, 'auth_token'],
        ADMIN: ['atk_' + STORAGE_VERSION, 'admin_token'],
        MANAGER: ['mtk_' + STORAGE_VERSION, 'server_manager_token']
    };
    
    const SS = {
        get: k => { try { return sessionStorage.getItem(k); } catch { return null; } },
        set: (k, v) => { try { sessionStorage.setItem(k, v); } catch {} },
        remove: k => { try { sessionStorage.removeItem(k); } catch {} },
        getJSON: k => { try { return JSON.parse(sessionStorage.getItem(k)); } catch { return null; } },
        setJSON: (k, v) => { try { sessionStorage.setItem(k, JSON.stringify(v)); } catch {} }
    };
    
    const LS = {
        get: k => { try { return localStorage.getItem(k); } catch { return null; } },
        set: (k, v) => { try { localStorage.setItem(k, v); } catch {} },
        remove: k => { try { localStorage.removeItem(k); } catch {} }
    };
    
    const Token = {
        get: keys => SS.get(keys[0]) || SS.get(keys[1]),
        set: (keys, v) => { SS.set(keys[0], v); SS.set(keys[1], v); },
        clear: keys => { SS.remove(keys[0]); SS.remove(keys[1]); }
    };
    
    const StorageManager = {
        
        session: {
            setUserId: id => SS.set('user_id', id),
            getUserId: () => SS.get('user_id') || '',
            
            setUserToken: t => Token.set(TOKEN_KEYS.USER, t),
            getUserToken: () => Token.get(TOKEN_KEYS.USER),
            
            setUsername: n => SS.set('username', n),
            getUsername: () => SS.get('username') || '',
            
            setLoginTime: t => SS.set('login_time', t),
            getLoginTime: () => { const t = SS.get('login_time'); return t ? parseInt(t) : null; },
            clearLoginTime: () => SS.remove('login_time'),
            
            setAdminId: id => SS.set('admin_id', id),
            getAdminId: () => SS.get('admin_id') || '',
            
            setAdminToken: t => Token.set(TOKEN_KEYS.ADMIN, t),
            getAdminToken: () => Token.get(TOKEN_KEYS.ADMIN),
            
            setServerManagerToken: t => Token.set(TOKEN_KEYS.MANAGER, t),
            getServerManagerToken: () => Token.get(TOKEN_KEYS.MANAGER),
            
            setCurrentManagerId: id => SS.set('currentManagerId', id),
            getCurrentManagerId: () => SS.get('currentManagerId') || '',
            
            isUserLoggedIn: () => !!Token.get(TOKEN_KEYS.USER),
            isAdminLoggedIn: () => !!Token.get(TOKEN_KEYS.ADMIN),
            isServerManagerLoggedIn: () => !!Token.get(TOKEN_KEYS.MANAGER),
            isSessionExpired: () => false,
            
            clear() {
                ['user_id', 'username', 'login_time', 'admin_id', 'currentManagerId'].forEach(SS.remove);
                Token.clear(TOKEN_KEYS.USER);
                Token.clear(TOKEN_KEYS.ADMIN);
                Token.clear(TOKEN_KEYS.MANAGER);
            }
        },
        
        preferences: {
            setTheme: v => LS.set('theme', v),
            getTheme: () => LS.get('theme') || 'dark',
            
            setLanguage: v => LS.set('language', v),
            getLanguage: () => LS.get('language') || 'zh-CN',
            
            setDisplayMode: v => LS.set('displayMode', v),
            getDisplayMode: () => LS.get('displayMode') || 'default',
            
            setApiBase: v => v ? LS.set('api_base', v) : LS.remove('api_base'),
            getApiBase: () => LS.get('api_base') || '',
            
            setGlobalExchangeRate: v => SS.set('globalExchangeRate', v),
            getGlobalExchangeRate: () => parseFloat(SS.get('globalExchangeRate')) || 30,
            
            setSaGlobalSend: v => SS.set('saGlobalSend', v),
            getSaGlobalSend: () => SS.get('saGlobalSend') || '0.00',
            
            setSaRatesGlobal: v => SS.setJSON('sa_rates_global', v),
            getSaRatesGlobal: () => SS.getJSON('sa_rates_global')
        },
        
        admin: {
            setAdminAccounts: v => SS.setJSON('adminAccounts', v),
            getAdminAccounts: () => SS.getJSON('adminAccounts') || [],
            
            setDeletedAdminIds: v => SS.setJSON('deletedAdminIds', v),
            getDeletedAdminIds: () => SS.getJSON('deletedAdminIds') || [],
            
            addDeletedAdminId(id) {
                const ids = this.getDeletedAdminIds();
                if (!ids.includes(id)) { ids.push(id); this.setDeletedAdminIds(ids); }
            }
        },
        
        user: {
            setUserInfo: (uid, v) => SS.setJSON('user_info_' + uid, v),
            getUserInfo: uid => SS.getJSON('user_info_' + uid) || {},
            clearUserInfo: uid => SS.remove('user_info_' + uid),
            
            setUserBackends: v => SS.setJSON('user_backends', v),
            getUserBackends: () => SS.getJSON('user_backends') || []
        },
        
        server: {
            setServerData: v => SS.setJSON('serverData', v),
            getServerData: () => SS.getJSON('serverData'),
            clearServerData: () => SS.remove('serverData')
        },
        
        task: {
            setTraceId: (tid, v) => SS.set('trace:' + tid, v),
            getTraceId: tid => SS.get('trace:' + tid) || '',
            clearTraceId: tid => SS.remove('trace:' + tid)
        },
        
        superAdmin: {
            setSuperAdminLoggedIn: v => SS.set('superAdminLoggedIn', v ? '1' : '0'),
            isSuperAdminLoggedIn: () => SS.get('superAdminLoggedIn') === '1'
        },
        
        clearPersistentLogin() { this.session.clear(); },
        logout() { this.session.clear(); },
        clearAll() { sessionStorage.clear(); localStorage.clear(); }
    };
    
    window.StorageManager = StorageManager;
    
    window.getAuthToken = () => StorageManager.session.getUserToken();
    window.setAuthToken = t => StorageManager.session.setUserToken(t);
    window.getUserId = () => StorageManager.session.getUserId();
    window.setUserId = id => StorageManager.session.setUserId(id);
    window.getUsername = () => StorageManager.session.getUsername();
    window.setUsername = n => StorageManager.session.setUsername(n);
    window.getLoginTime = () => StorageManager.session.getLoginTime();
    window.setLoginTime = t => StorageManager.session.setLoginTime(t);
    window.clearLoginTime = () => StorageManager.session.clearLoginTime();
    window.getAdminId = () => StorageManager.session.getAdminId();
    window.setAdminId = id => StorageManager.session.setAdminId(id);
    window.getAdminToken = () => StorageManager.session.getAdminToken();
    window.setAdminToken = t => StorageManager.session.setAdminToken(t);
    window.getServerManagerToken = () => StorageManager.session.getServerManagerToken();
    window.setServerManagerToken = t => StorageManager.session.setServerManagerToken(t);
    window.isSessionExpired = () => false;
    window.isSessionTimeout = () => false;
    window.getAdminAccounts = () => StorageManager.admin.getAdminAccounts();
    window.setAdminAccounts = v => StorageManager.admin.setAdminAccounts(v);
    window.getSaGlobalSend = () => StorageManager.preferences.getSaGlobalSend();
    window.setSaGlobalSend = v => StorageManager.preferences.setSaGlobalSend(v);
    window.getSaRatesGlobal = () => StorageManager.preferences.getSaRatesGlobal();
    window.setSaRatesGlobal = v => StorageManager.preferences.setSaRatesGlobal(v);
    window.logoutUser = () => StorageManager.session.clear();
    window.logoutAdmin = () => StorageManager.session.clear();
    window.logoutServerManager = () => StorageManager.session.clear();
    window.logoutAll = () => StorageManager.session.clear();
    window.checkAuthStatus = () => ({
        hasUserAuth: !!StorageManager.session.getUserToken(),
        hasAdminAuth: !!StorageManager.session.getAdminToken(),
        hasManagerAuth: !!StorageManager.session.getServerManagerToken(),
        isTimeout: false
    });
    window.handleSessionTimeout = () => false;
    
    window.Storage = {
        Auth: { getToken: window.getAuthToken, setToken: window.setAuthToken },
        AdminAuth: { getToken: window.getAdminToken, setToken: window.setAdminToken },
        ManagerAuth: { getToken: window.getServerManagerToken, setToken: window.setServerManagerToken },
        logoutAll: window.logoutAll,
        isLoggedIn: () => !!StorageManager.session.getUserToken(),
        isAdminLoggedIn: () => !!StorageManager.session.getAdminToken(),
        isManagerLoggedIn: () => !!StorageManager.session.getServerManagerToken(),
        version: STORAGE_VERSION
    };
    
})();
