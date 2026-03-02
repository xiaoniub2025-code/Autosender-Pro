#!/usr/bin/env python3
# region *****************************     加载库                 *******************************

from PyQt5.QtWidgets import QApplication,QWidget,QLabel,QTextEdit,QPushButton,QStackedWidget,QVBoxLayout,QHBoxLayout,QListWidget,QLineEdit,QMessageBox,QFrame,QListWidgetItem,QFileDialog,QMenu,QSizePolicy,QScrollArea,QDialog
from PyQt5.QtCore import Qt,QPoint,QEvent,QObject,QTimer,pyqtSignal,QRect,QThread,QRectF
from PyQt5.QtGui import QFont, QPixmap, QPalette, QIcon, QPainter, QColor, QPainterPath,QRegion
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning) 
import sys
import os
import json
import asyncio
import subprocess
import threading
import time
import aiohttp
import sqlite3
import ssl
import tempfile
import shutil
import logging
from typing import Any
from datetime import timedelta, timezone, datetime
from aiohttp import web
from pathlib import Path
import builtins
import socket
import queue
from collections import deque
import atexit
import platform

# endregion

# region *****************************     路径设置                 *******************************

# 数据库路径(本地)
db_path = os.path.expanduser("~/Library/Messages/chat.db")

def get_app_data_dir(app_name: str = "Autosender Pro") -> str:
    system = platform.system()
    if system == "Darwin":
        base_dir = os.path.expanduser("~/Library/Application Support")
    elif system == "Windows":
        base_dir = os.getenv("LOCALAPPDATA", os.path.expanduser("~/AppData/Local"))
    else:
        base_dir = os.path.expanduser("~/.config")
    app_dir = os.path.join(base_dir, app_name)
    os.makedirs(app_dir, exist_ok=True)
    return app_dir

def resource_path(relative_path: str) -> str:
    if hasattr(sys, "_MEIPASS"):
        base_path = sys._MEIPASS
    else:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)
# endregion

# region *****************************     全局默认配置           ********************************

SEND_MESSAGE_INTERVAL = 1.0
TASK_CACHE_TTL = 300
MAX_TASK_CACHE_SIZE = 100
COCOA_EPOCH_OFFSET = 978307200
SEND_MESSAGE_TIMEOUT = 20
MAX_PROCESSED_SHARDS = 10000
SHARD_CLEANUP_RATIO = 2

# endregion

# region ******************************    日志系统               *******************************
WRITE_INTERVAL = 3
MAX_BUFFER = 50
_log_queue = queue.Queue(maxsize=10000)
_log_file = None
_signals_ref = None
_ws_ref = None
_main_loop = None
_log_started = False
_stop_event = threading.Event()

def now_iso():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def set_main_loop(loop):
    global _main_loop
    _main_loop = loop

def set_signals(obj):
    global _signals_ref
    _signals_ref = obj

def set_ws_ref(ws):
    """设置 WebSocket 对象 (连接远程后调用)"""
    global _ws_ref
    _ws_ref = ws

def _init_log():
    global _log_file
    log_dir = get_app_data_dir("Autosender Pro")
    hostname = socket.gethostname().split("-")[0]
    date_str = datetime.now().strftime("%Y%m%d")
    _log_file = os.path.join(log_dir, f"worker_{hostname}_{date_str}.log")

def _log_worker():
    buffer = []
    last_write = time.time()
    while not _stop_event.is_set():
        try:
            try:
                item = _log_queue.get(timeout=0.5)
                buffer.append(item)
            except queue.Empty:
                pass
            should_write = len(buffer) >= MAX_BUFFER or (buffer and time.time() - last_write >= WRITE_INTERVAL)
            if should_write and buffer:
                # 1. 写本地文件
                if _log_file:
                    try:
                        with open(_log_file, "a", encoding="utf-8") as f:
                            f.write("\n".join(buffer) + "\n")
                    except: pass
                # 2. 本地 GUI 显示
                if _signals_ref:
                    try:
                        for line in buffer:
                            _signals_ref.log.emit(line)
                    except: pass
                # 3. 远程 HTML 面板显示
                if _ws_ref and _main_loop:
                    try:
                        for line in buffer:
                            _main_loop.call_soon_threadsafe(
                                asyncio.ensure_future,
                                _ws_ref.send_json({"type": "worker_log", "line": line})
                            )
                    except: pass
                buffer.clear()
                last_write = time.time()
        except:
            time.sleep(0.1)

def log(*args):
    global _log_started
    try:
        content = " ".join(str(a) for a in args) if len(args) > 1 else str(args[0])
        log_line = f"[{datetime.now().strftime('%H:%M:%S')}] {content}"
    except:
        log_line = "[Log Error]"
    print(log_line)
    if not _log_started:
        _log_started = True
        threading.Thread(target=_log_worker, daemon=True, name="LogWorker").start()
    try:
        _log_queue.put_nowait(log_line)
    except queue.Full:
        pass

def stop_log():
    _stop_event.set()

def start_async_backend():
    """后台异步循环，用于远程日志发送"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    set_main_loop(loop)
    loop.run_forever()

# 初始化

_init_log()


# endregion

# region *****************************     GUI 配置组件(全局)      *******************************

# placeholder
class myplaceholder(QTextEdit):


    def __init__(self, placeholder="", parent=None, placeholder_font_size=10):
        super().__init__(parent)
        self.placeholder = placeholder
        self.placeholder_font_size = placeholder_font_size
        self.placeholder_color = QColor("#888888")
        # 不设置输入框样式，样式由外部控制
        self.textChanged.connect(lambda: self.update())

    def paintEvent(self, event):
        super().paintEvent(event)
        if not self.toPlainText().strip():
            painter = QPainter(self.viewport())
            font = QFont()
            font.setPointSize(int(self.placeholder_font_size))
            painter.setFont(font)
            painter.setPen(self.placeholder_color)
            painter.drawText(5, 18, self.placeholder)
            painter.end()

# 静音通知弹窗
class SilentNotification(QWidget):

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowFlags(Qt.FramelessWindowHint | Qt.Tool | Qt.WindowStaysOnTopHint)
        self.setAttribute(Qt.WA_TranslucentBackground)
        self.setAttribute(Qt.WA_ShowWithoutActivating)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        content = QFrame()
        content.setStyleSheet(
            f"QFrame {{ background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 rgba(255, 248, 231, 0.98), stop:1 rgba(255, 245, 220, 0.95)); border: 3px solid #000000; border-radius: 18px; padding: 20px; }}"
        )
        content_layout = QVBoxLayout(content)
        content_layout.setSpacing(5)
        content_layout.setContentsMargins(12, 8, 12, 8)
        row1 = QHBoxLayout()
        row1.setSpacing(8)
        title = QLabel("智能登录已开启")
        title.setStyleSheet(
            f"QLabel {{ color: #2F2F2F; font-family: 'Comic Sans MS', 'Yuanti SC', 'STHeiti'; font-weight: bold; font-size: 13px; font-weight: bold; background: transparent; border: none; }}"
        )
        row1.addWidget(title)
        features = QLabel("自动检测 重连 修复 更换")
        features.setStyleSheet(
            f"QLabel {{ color: rgba(47, 47, 47, 0.6); font-family: 'Comic Sans MS', 'Yuanti SC', 'STHeiti'; font-weight: bold; font-size: 10px; background: transparent; border: none; }}"
        )
        row1.addWidget(features)
        content_layout.addLayout(row1)
        hint = QLabel("请确保账号列表已保存足够的 Apple ID")
        hint.setStyleSheet(
            f"QLabel {{ color: #2F2F2F; font-family: 'Comic Sans MS', 'Yuanti SC', 'STHeiti'; font-weight: bold; font-size: 11px; background: transparent; border: none; }}"
        )
        content_layout.addWidget(hint)
        layout.addWidget(content)
        self.adjustSize()
        QTimer.singleShot(3000, self.fade_out)

    def fade_out(self):
        self.close()

    def showEvent(self, event):
        super().showEvent(event)
        if self.parent():
            parent = self.parent()
            parent_global_pos = parent.mapToGlobal(parent.rect().center())
            x = parent_global_pos.x() - self.width() // 2
            y = parent_global_pos.y() - self.height() // 2
            self.move(x, y)

# 提示弹窗
class SimpleNotification(QWidget):

    def __init__(self, message, parent=None):
        super().__init__(parent)
        self.setWindowFlags(Qt.FramelessWindowHint | Qt.Tool | Qt.WindowStaysOnTopHint)
        self.setAttribute(Qt.WA_TranslucentBackground)
        self.setAttribute(Qt.WA_ShowWithoutActivating)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        content = QFrame()
        content.setStyleSheet(
            f"QFrame {{ background: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 rgba(255, 214, 231, 0.95), stop:1 rgba(193, 240, 255, 0.90)); border-radius: 15px; border: 2px solid rgba(255, 255, 255, 0.3); padding: 15px 25px; }}"
        )
        content_layout = QVBoxLayout(content)
        content_layout.setContentsMargins(10, 10, 10, 10)
        label = QLabel(message)
        label.setAlignment(Qt.AlignCenter)
        label.setStyleSheet(
            "QLabel { color: #2F2F2F; font-size: 13px; font-weight: 500; background: transparent; border: none; }"
        )
        content_layout.addWidget(label)
        layout.addWidget(content)
        self.adjustSize()
        QTimer.singleShot(2000, self.close)

    def showEvent(self, event):
        super().showEvent(event)
        if self.parent():
            parent = self.parent()
            parent_global_pos = parent.mapToGlobal(parent.rect().center())
            x = parent_global_pos.x() - self.width() // 2
            y = parent_global_pos.y() - self.height() // 2
            self.move(x, y)

# 计数器的文本编辑框
class TextEditWithCounter(QWidget):

    def __init__(
        self,
        placeholder="",
        is_phone_counter=False,
        parent=None,
        placeholder_font_size=10,
    ):
        super().__init__(parent)
        self.is_phone_counter = is_phone_counter
        self.text_edit = myplaceholder(
            placeholder, self, placeholder_font_size=placeholder_font_size
        )
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)
        layout.addWidget(self.text_edit)
        self.counter_label = QLabel("", self.text_edit)
        self.counter_label.setStyleSheet(
            "background: rgba(255, 255, 255, 0.95); border: 1px solid rgba(0,0,0,0.2); color: #2F2F2F; font-size: 10px; padding: 2px 6px; font-weight: bold; border-radius: 3px;"
        )
        self.counter_label.setAlignment(Qt.AlignCenter)
        self.counter_label.raise_()
        self.update_counter()
        self.text_edit.textChanged.connect(self.update_counter)

    def update_counter(self):
        text = self.text_edit.toPlainText()
        if self.is_phone_counter:
            lines = [line.strip() for line in text.split("\n") if line.strip()]
            count = 0
            for line in lines:
                if "," in line:
                    parts = [p.strip() for p in line.split(",") if p.strip()]
                    count += len(parts)
                else:
                    count += 1
            self.counter_label.setText(f"号码: {count}")
        else:
            count = 0
            for char in text:
                code = ord(char)
                if 0x4E00 <= code <= 0x9FFF:
                    count += 2
                else:
                    count += 1
            self.counter_label.setText(f"字符: {count}")
        self.counter_label.adjustSize()
        QTimer.singleShot(10, self._update_counter_position)

    def _update_counter_position(self):
        if self.counter_label and self.text_edit:
            margin = 5
            label_width = self.counter_label.width()
            label_height = self.counter_label.height()
            self.counter_label.move(
                self.text_edit.width() - label_width - margin,
                self.text_edit.height() - label_height - margin,
            )

    def resizeEvent(self, event):
        super().resizeEvent(event)
        QTimer.singleShot(10, self._update_counter_position)

    def toPlainText(self):
        return self.text_edit.toPlainText()

    def setText(self, text):
        self.text_edit.setText(text)

    def clear(self):
        self.text_edit.clear()

    def __getattr__(self, name):
        if hasattr(self.text_edit, name):
            return getattr(self.text_edit, name)
        raise AttributeError(
            f"'{type(self).__name__}' object has no attribute '{name}'"
        )

# 固定尺寸面板
class FixedSizePanel(QFrame):

    def __init__(self, color, width, height, parent=None):
        super().__init__(parent)
        self._color = color
        self._width = width
        self._height = height
        self._is_percentage = (isinstance(width, float) and 0 < width <= 1) or (
            isinstance(height, float) and 0 < height <= 1
        )
        if self._is_percentage:
            self._update_size_from_parent()
        else:
            self.setFixedSize(int(width), int(height))

        # 样式直接内联
        # 判断是否是渐变（包含qlineargradient）
        if "qlineargradient" in str(self._color):
            background = f"background: {self._color};"
        else:
            background = f"background-color: {self._color};"

        self.setStyleSheet(
            f"""
            {background}
            border: 2px solid #000000; 
            border-radius: 18px;
        """
        )

# 根据父窗口大小更新尺寸（百分比模式
    def _update_size_from_parent(self):

        if not self.parent():
            return

        # 找到真正的父窗口（可能是 CenteredContainer 或 MainWindow）
        parent = self.parent()
        while parent:
            if isinstance(parent, QWidget):
                # 找到 MainWindow 或最顶层的 QWidget
                if hasattr(parent, "width") and parent.width() > 0:
                    break
            parent = parent.parent()

        if parent and hasattr(parent, "width"):
            parent_width = parent.width()
            parent_height = parent.height()

            if parent_width > 0 and parent_height > 0:
                # 计算实际尺寸
                if isinstance(self._width, float) and 0 < self._width <= 1:
                    actual_width = int(parent_width * self._width)
                else:
                    actual_width = int(self._width)

                if isinstance(self._height, float) and 0 < self._height <= 1:
                    actual_height = int(parent_height * self._height)
                else:
                    actual_height = int(self._height)

                self.setFixedSize(actual_width, actual_height)

# 窗口显示时更新尺寸（百分比模式)
    def showEvent(self, event):

        if self._is_percentage:
            # 延迟更新，确保父窗口大小已确定
            QTimer.singleShot(0, self._update_size_from_parent)
        super().showEvent(event)

# 窗口大小改变时更新尺寸（百分比模式）
    def resizeEvent(self, event):
        
        if self._is_percentage:
            self._update_size_from_parent()
        super().resizeEvent(event)

# 居中容器
class CenteredContainer(QWidget):  

    def __init__(self, panel):
        super().__init__()
        # 确保容器本身透明、无框
        # 使用Style类统一管理样式
        self.setStyleSheet("background: transparent; border: none;")
        layout = QVBoxLayout(self)
        layout.setAlignment(Qt.AlignCenter)  # 【核心】强制居中
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(panel)

# 通用功能按钮
class ActionBtn(QPushButton):
    """通用功能按钮"""

    def __init__(self, text, color="#FFFFFF", w=None, h=35, radius=8):
        super().__init__(text)
        self.setCursor(Qt.PointingHandCursor)
        if w:
            self.setFixedWidth(w)
        self.setFixedHeight(h)
        self.setStyleSheet(
            "QPushButton { background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 rgba(255, 255, 255, 0.95), stop:0.5 rgba(255, 240, 200, 0.9), stop:1 rgba(255, 255, 255, 0.85)); border: 2px solid #000000; border-radius: 15px; font-family: 'Comic Sans MS', 'Yuanti SC', 'STHeiti'; font-weight: bold; font-size: 12px; color: #2F2F2F; font-weight: bold; } QPushButton:hover { background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 rgba(255, 255, 200, 0.95), stop:0.5 rgba(255, 220, 150, 0.9), stop:1 rgba(255, 255, 200, 0.85)); border-color: #FFD700; border-width: 3px; } QPushButton:pressed { background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 rgba(255, 200, 150, 0.95), stop:0.5 rgba(255, 180, 120, 0.9), stop:1 rgba(255, 200, 150, 0.85)); border-color: #FF8C00; border-width: 2px; }"
            + f"QPushButton {{ background-color: {color}; border-radius: {radius}px; }}"
        )

# endregion

# region *****************************     PanelMain & Welcome   ********************************


class MainWindow(QWidget):

    def __init__(self):
        super().__init__()
        self.oldPos = self.pos()

        icon_path = resource_path("icns.icns")
        if os.path.exists(icon_path):
            self.setWindowIcon(QIcon(icon_path))

        self.initUI()
        self.set_position(245, 105)

    def set_position(self, x=245, y=105):
        self.move(x, y)

    def initUI(self):

        self.setFixedSize(750, 550)  # 固定窗口尺寸，不允许调整
        self.setWindowFlags(Qt.FramelessWindowHint)
        self.setAttribute(Qt.WA_TranslucentBackground)
        self.setStyleSheet("QWidget { outline: none; } QStackedWidget { background: transparent; border: none; } QLabel { color: #2F2F2F; background: transparent; } QPushButton { color: #2F2F2F; } QLineEdit, QTextEdit, QListWidget { border: 2px solid #000000; border-radius: 10px; background-color: #FFFFFF; padding: 5px; color: #2F2F2F; font-family: 'Comic Sans MS', 'Yuanti SC', 'STHeiti'; font-weight: bold; font-size: 13px; } QScrollBar:vertical { width: 0px; } QMenu { background-color: #FFFFFF; border: 2px solid #000000; border-radius: 12px; padding: 4px; } QMenu::item { color: #2F2F2F; padding: 6px 20px; border-radius: 4px; } QMenu::item:selected { background-color: rgba(139, 0, 255, 0.2); color: #2F2F2F; } QMenu::item:disabled { color: #999999; }")

        # 2. 外层布局
        outer_layout = QVBoxLayout(self)
        outer_layout.setContentsMargins(10, 10, 10, 10)

        self.main_frame = QFrame()
        # 样式直接内联
        self.main_frame.setStyleSheet(
            f"""
            background-color: #FFF8E7; 
            border: 3px solid #000000; 
            border-radius: 18px;
            margin: 0px;
        """
        )
        outer_layout.addWidget(self.main_frame)

        inner_layout = QVBoxLayout(self.main_frame)
        inner_layout.setContentsMargins(0, 0, 0, 20)
        inner_layout.setSpacing(0)

        # 3. 顶部标题栏 - 先创建PanelBackend以便按钮能连接
        self.panel_backend = PanelBackend(self)

        content_layout = QHBoxLayout()
        content_layout.setContentsMargins(20, 20, 20, 0)
        content_layout.setSpacing(20)

        # --- 左侧 Sidebar ---
        sidebar = QVBoxLayout()
        sidebar.setSpacing(30)
        sidebar.addSpacing(30)

        btn_config = [
            ("服务器", "#C8E6C9"),
            ("iMessage", "#FFE082"),
            ("收件箱", "#FFB74D"),
            ("登录助手", "#90CAF9"),
            ("修复工具", "#F48FB1"),
            ("系统文件", "#CE93D8"),
        ]

        self.nav_btns = {}
        self.nav_btn_colors = {}  # 存储每个按钮的颜色
        self.current_nav_btn = None  # 当前选中的按钮

        for text, color in btn_config:
            btn = QPushButton(text)
            btn.setFixedHeight(40)
            btn.setCursor(Qt.PointingHandCursor)

            # 存储颜色
            self.nav_btn_colors[text] = color

            # 设置样式
            btn.setStyleSheet(
                "QPushButton { background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #b2ff9be6, stop:0.5 #d0ff01, stop:1 #9bff7a); border: 2px solid #000000; border-radius: 15px; color: #2F2F2F; font-family: 'Comic Sans MS', 'Yuanti SC', 'STHeiti'; font-weight: bold; font-size: 15px; } QPushButton:hover { background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #d0fcc4, stop:0.5 #2eef68, stop:1 #02ff0a); border-radius: 15px; margin-top: 2px; margin-left: 2px; }" + f"QPushButton{{background-color:{color};}}"
            )
            sidebar.addWidget(btn)
            self.nav_btns[text] = btn

        sidebar.addStretch()

        self.stack = QStackedWidget()
        self.stack.setStyleSheet("background: transparent; border: none;")

        self.panel_welcome = PanelWelcome(self)
        self.stack.addWidget(CenteredContainer(self.panel_welcome))

        self.stack.addWidget(CenteredContainer(self.panel_backend))

        self.panel_sms = PanelIMessage(self)
        self.stack.addWidget(CenteredContainer(self.panel_sms))

        self.panel_inbox = PanelInbox(self)
        self.stack.addWidget(CenteredContainer(self.panel_inbox))

        self.panel_id = PanelID(self)
        self.stack.addWidget(CenteredContainer(self.panel_id))

        self.panel_tools = PanelTools(self)
        self.stack.addWidget(CenteredContainer(self.panel_tools))

        self.nav_btns["服务器"].clicked.connect(lambda: self.switch_page("服务器", 1))
        self.nav_btns["iMessage"].clicked.connect(
            lambda: self.switch_page("iMessage", 2)
        )
        self.nav_btns["收件箱"].clicked.connect(lambda: self.switch_page("收件箱", 3))
        self.nav_btns["登录助手"].clicked.connect(
            lambda: self.switch_page("登录助手", 4)
        )
        self.nav_btns["修复工具"].clicked.connect(
            lambda: self.switch_page("修复工具", 5)
        )
        self.nav_btns["系统文件"].clicked.connect(self.open_log_folder)

        # 现在设置标题栏（在panel_backend创建之后）
        self.setup_title_bar(inner_layout)

        # 组装布局
        content_layout.addLayout(sidebar, 1)
        content_layout.addWidget(self.stack, 4)
        inner_layout.addLayout(content_layout)

        # 默认显示欢迎页
        self.stack.setCurrentIndex(0)

    def set_icon(self, icon_name="icns.icns"):
        """设置窗口图标"""
        icon_path = resource_path(icon_name)
        if os.path.exists(icon_path):
            self.setWindowIcon(QIcon(icon_path))

    def setup_title_bar(self, parent_layout):
        title_bar = QFrame()
        title_bar.setFixedHeight(35)
        # 样式直接内联
        title_bar.setStyleSheet(
            f"""
            background: #DCE775; 
            border-top: none;
            border-left: none;
            border-right: none;
            border-bottom: 3px solid #000000;
            border-top-left-radius: 5px;
            border-top-right-radius: 5px;
            border-bottom-left-radius: 0px;
            border-bottom-right-radius: 0px;
        """
        )
        layout = QHBoxLayout(title_bar)
        layout.setContentsMargins(15, 3, 15, 3)
        # 确保标题栏填充整个宽度
        title_bar.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)

        layout.addWidget(
            QLabel(
                "AutoSender Pro",
                styleSheet=f"border:none; font-family: 'Comic Sans MS', 'Yuanti SC', 'STHeiti'; font-weight: bold; font-size:18px ; font-weight: bold;",
            )
        )
        layout.addStretch()

        btn_min = QPushButton("-")
        btn_min.setFixedSize(25, 25)
        btn_min.setStyleSheet(
            """
            QPushButton {
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 rgba(255, 236, 210, 0.95), stop:0.6 rgba(252, 182, 159, 0.92), stop:1 rgba(255, 179, 71, 0.95));
                border: 2px solid #2F2F2F;
                border-radius: 12px;
                color: #2F2F2F;
                font-weight: bold;
                font-size: 14px;
                font-family: 'Comic Sans MS';
            }
            QPushButton:hover {
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 rgba(240, 250, 255, 0.95), stop:0.6 rgba(255, 240, 250, 0.92), stop:1 rgba(255, 250, 255, 0.95));
            }
            QPushButton:pressed {
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 rgba(255, 200, 220, 0.92), stop:0.6 rgba(255, 214, 231, 0.90), stop:1 rgba(193, 240, 255, 0.90));
            }
            """
        )
        # 使用简单的transform实现抖动效果（不改变按钮大小）
        original_pos_min = None

        def enter_min(e):
            nonlocal original_pos_min
            original_pos_min = btn_min.pos()
            btn_min.move(btn_min.x() + 2, btn_min.y() + 2)
            super(QPushButton, btn_min).enterEvent(e)

        def leave_min(e):
            if original_pos_min:
                btn_min.move(original_pos_min.x(), original_pos_min.y())
            super(QPushButton, btn_min).leaveEvent(e)

        btn_min.enterEvent = enter_min
        btn_min.leaveEvent = leave_min
        btn_min.clicked.connect(self.showMinimized)

        btn_close = QPushButton("×")
        btn_close.setFixedSize(25, 25)
        btn_close.setStyleSheet(
            """
            QPushButton {
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 rgba(255, 107, 53, 0.95), stop:0.55 rgba(255, 31, 112, 0.92), stop:1 rgba(245, 11, 206, 0.92));
                border: 2px solid #2F2F2F;
                border-radius: 12px;
                color: #FFFFFF;
                font-weight: bold;
                font-size: 18px;
                font-family: 'Comic Sans MS', Yuanti SC;
            }
            QPushButton:hover {
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 rgba(255, 230, 240, 0.92), stop:0.6 rgba(255, 210, 230, 0.90), stop:1 rgba(220, 240, 255, 0.92));
            }
            QPushButton:pressed {
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 rgba(255, 200, 220, 0.92), stop:0.6 rgba(255, 154, 162, 0.90), stop:1 rgba(255, 179, 186, 0.90));
            }
            """
        )
        # 使用简单的transform实现抖动效果（不改变按钮大小）
        original_pos_close = None

        def enter_close(e):
            nonlocal original_pos_close
            original_pos_close = btn_close.pos()
            btn_close.move(btn_close.x() + 2, btn_close.y() + 2)
            super(QPushButton, btn_close).enterEvent(e)

        def leave_close(e):
            if original_pos_close:
                btn_close.move(original_pos_close.x(), original_pos_close.y())
            super(QPushButton, btn_close).leaveEvent(e)

        btn_close.enterEvent = enter_close
        btn_close.leaveEvent = leave_close
        btn_close.clicked.connect(self.close)

        layout.addWidget(btn_min)
        layout.addSpacing(10)
        layout.addWidget(btn_close)
        parent_layout.addWidget(title_bar)

    def switch_page(self, btn_name, page_index):
        """切换页面并更新按钮选中状态"""
        # 切换页面
        self.stack.setCurrentIndex(page_index)

        # 重置所有按钮为未选中状态
        for name, btn in self.nav_btns.items():
            if name == "日志文件":  # 日志文件按钮不参与页面切换
                continue
            color = self.nav_btn_colors[name]
            btn.setStyleSheet(
                "QPushButton { background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #b2ff9be6, stop:0.5 #d0ff01, stop:1 #9bff7a); border: 2px solid #000000; border-radius: 12px; color: #2F2F2F; font-family: 'Comic Sans MS', 'Yuanti SC', 'STHeiti'; font-weight: bold; font-size: 15px; } QPushButton:hover { background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #d0fcc4, stop:0.5 #2eef68, stop:1 #02ff0a); border-radius: 12px; margin-top: 2px; margin-left: 2px; }" + f"QPushButton{{background-color:{color};}}"
            )

        # 设置当前按钮为选中状态（更亮的颜色）
        if btn_name in self.nav_btns and btn_name != "日志文件":
            btn = self.nav_btns[btn_name]
            color = self.nav_btn_colors[btn_name]
            # 使用悬停效果作为选中状态
            btn.setStyleSheet(
                "QPushButton { background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #b2ff9be6, stop:0.5 #d0ff01, stop:1 #9bff7a); border: 2px solid #000000; border-radius: 12px; color: #2F2F2F; font-family: 'Comic Sans MS', 'Yuanti SC', 'STHeiti'; font-weight: bold; font-size: 15px; } QPushButton:hover { background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #d0fcc4, stop:0.5 #2eef68, stop:1 #02ff0a); border-radius: 12px; margin-top: 2px; margin-left: 2px; }"
                + f"""
                QPushButton {{
                    background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #d0fcc4, stop:0.5 #2eef68, stop:1 #02ff0a);
                    border-radius: 12px;
                    margin-top: 2px;
                    margin-left: 2px;
                }}
                """
            )
            self.current_nav_btn = btn_name

    def open_log_folder(self):
        """打开日志文件夹"""
        log_dir = get_app_data_dir("Autosender Pro")
        os.makedirs(log_dir, exist_ok=True)
        subprocess.Popen(["open", log_dir])

    def mousePressEvent(self, event):
        if event.button() == Qt.LeftButton:
            self.oldPos = event.globalPos()

    def mouseMoveEvent(self, event):
        if event.buttons() == Qt.LeftButton:
            delta = QPoint(event.globalPos() - self.oldPos)
            self.move(self.x() + delta.x(), self.y() + delta.y())
            self.oldPos = event.globalPos()

    def showEvent(self, event):
        """窗口显示时，确保居中显示"""
        super().showEvent(event)
        # 延迟居中，确保窗口大小已确定
        # QTimer.singleShot(50, self.center_on_screen)

    def resizeEvent(self, event):
        """窗口大小改变时，通知所有百分比面板更新尺寸"""
        super().resizeEvent(event)
        # 延迟更新，确保布局已完成
        QTimer.singleShot(10, self._update_percentage_panels)

    def _update_percentage_panels(self):
        """更新所有使用百分比尺寸的面板"""

        def update_widget(widget):
            """递归更新所有 FixedSizePanel"""
            if (
                isinstance(widget, FixedSizePanel)
                and hasattr(widget, "_is_percentage")
                and widget._is_percentage
            ):
                widget._update_size_from_parent()
            # 递归处理子控件
            for child in widget.findChildren(QWidget):
                if (
                    isinstance(child, FixedSizePanel)
                    and hasattr(child, "_is_percentage")
                    and child._is_percentage
                ):
                    child._update_size_from_parent()

        # 更新所有子控件
        for widget in self.findChildren(FixedSizePanel):
            if hasattr(widget, "_is_percentage") and widget._is_percentage:
                widget._update_size_from_parent()


class PanelWelcome(FixedSizePanel):

    def __init__(self, parent_window):
        # 渐变背景
        gradient_bg = "qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #FFF8E7, stop:0.5 #FFF9C4, stop:1 #FFF59D)"
        super().__init__(gradient_bg, 550, 430, parent_window)
        self.main_window = parent_window

        # 去掉边框
        self.setStyleSheet("QFrame { border: none; }")

        # 布局：居中显示
        layout = QVBoxLayout(self)
        layout.setAlignment(Qt.AlignCenter)
        layout.setContentsMargins(0, 0, 0, 0)

        # 创建标签用于显示图片
        self.image_label = QLabel()
        self.image_label.setAlignment(Qt.AlignCenter)
        self.image_label.setStyleSheet("background: transparent; border: none;")

        # 加载并显示图片
        self.load_image("bg.png")

        # 将图片标签添加到布局中
        layout.addWidget(self.image_label)

    def load_image(self, image_path):
        """加载并显示图片"""
        # 使用 resource_path 获取正确的路径
        full_path = resource_path(image_path)

        # 创建QPixmap对象
        pixmap = QPixmap(full_path)

        # 检查图片是否成功加载
        if pixmap.isNull():
            return

        # 调整图片大小以适应面板，保持宽高比
        scaled_pixmap = pixmap.scaled(
            550,  # 面板宽度
            430,  # 面板高度
            Qt.KeepAspectRatio,
            Qt.SmoothTransformation,
        )

        # 设置图片到标签
        self.image_label.setPixmap(scaled_pixmap)


# endregion

# region *****************************     后端服务器             ********************************

# 定义服务器相关的PyQt信号
class ServerSignals(QObject):

    update_ui = pyqtSignal()
    log = pyqtSignal(str)
    task_record = pyqtSignal(int, int, int)
    super_admin_command = pyqtSignal(str, dict)  # action, params

    def __init__(self):
        super().__init__()

# 在后台线程中运行异步服务器
class ServerWorker(QThread):
    error = pyqtSignal(str)

    def __init__(self, panel):
        super().__init__()
        self.panel = panel

    def run(self):
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(self.panel.run_async_server_ws())
            finally:
                loop.close()
        except Exception as e:
            self.error.emit(str(e))

# Worker服务器 

class AutoSenderServer:

    # region  worker 系统初始化

    def __init__(self):
        self.sending = False
        self.config_dir = get_app_data_dir("Autosender Pro")
        os.makedirs(self.config_dir, exist_ok=True)
        self._ssl_connector = None
        self.signals = None
        self.ws_clients = set()
        self.ws_client_info = {}
        self.client_info = {}
        self.inbox_checker_task = None
        self._inbox_checker_running_lock = None
        self.server_id = None
        self.server_port = None
        self.server_url = None
        self.server_phone = None
        self.api_base_url = os.getenv(
            "API_BASE_URL", "https://autosender.up.railway.app/api"
        )
        try:
            self.credits_per_message = float(os.getenv("CREDITS_PER_MESSAGE", "1.0"))
        except Exception:
            self.credits_per_message = 1.0
        self.worker_ws_task = None
        self.worker_ws_running = False
        self.worker_ws = None
        self._session = None
        self._task_info_cache = {}
        self._test_task_received = False
        self._test_task_id = None
        self._test_api_task_id = None  # API返回的task_id
        self._processed_shards = set()  # 已处理的shard

        # 🔥 Worker日志相关
        self.log_sending = False  # 是否正在发送日志
        self.log_target_admin_id = None  # 目标管理员ID
        self.log_send_task = None  # 日志发送任务
        self.log_file_position = {}  # 记录每个日志文件的读取位置

    # 获取SSL连接器
    def _get_ssl_connector(self):
        if self._ssl_connector is None:
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
            self._ssl_connector = aiohttp.TCPConnector(ssl=ssl_context)
        return self._ssl_connector

    # 获取aiohttp Session
    async def _get_session(self):
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(connector=self._get_ssl_connector())
        return self._session

    # 关闭session
    async def _close_session(self):
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    # 构建API WebSocket URL
    def _build_api_ws_url(self, path: str) -> str:
        base = (self.api_base_url or "").strip().rstrip("/")

        if base.startswith("https://"):
            base = "wss://" + base[len("https://") :]
        elif base.startswith("http://"):
            base = "ws://" + base[len("http://") :]
        elif not (base.startswith("ws://") or base.startswith("wss://")):
            if "localhost" in base or "127.0.0.1" in base:
                base = "ws://" + base
            else:
                base = "wss://" + base
        if base.endswith("/api"):
            base = base[:-4]

        base = base.rstrip("/")
        if not path.startswith("/"):
            path = "/" + path

        final_url = base + path
        return final_url

    # endregion

    # region  服务端 通信状态 连接 心跳 远程交互

    # 启动worker WebSocket连接
    async def start_worker_ws(self):

        if self.worker_ws_running:
            return
        if not self.api_base_url:
            log(f"[{now_iso()}][WORKER][erro][1281][start_worker_ws][API地址未配置]")
            return
        self.worker_ws_running = True
        self.worker_ws_task = asyncio.create_task(self._worker_ws_loop())
        log(f"[{now_iso()}] Worker WebSocket 已启动")

    # 停止worker WebSocket
    async def stop_worker_ws(self):
        self.worker_ws_running = False
        try:
            if self.worker_ws is not None:
                await self.worker_ws.close()
        except:
            pass
        self.worker_ws = None
        if self.worker_ws_task:
            self.worker_ws_task.cancel()
            try:
                await self.worker_ws_task
            except:
                pass
        self.worker_ws_task = None
        log(f"[{now_iso()}] Worker WebSocket 已停止")

    # 计算worker就绪状态 - 只检查能否发送iMessage
    def _compute_ready_payload(self) -> dict:
        try:
            result = subprocess.run(
                ["osascript", "-e", 'return "ok"'],
                capture_output=True,
                text=True,
                timeout=3,
            )
            ready = result.returncode == 0
        except:
            ready = False

        message = "ready" if ready else "not_ready:osascript_failed"
        return {"ready": ready, "message": message}

    # Worker WebSocket主循环 - 处理连接、消息和心跳
    async def _worker_ws_loop(self):
        global _ws_ref
        ws_url = self._build_api_ws_url("/ws/worker")
        server_id = getattr(self, "server_id", None)
        while self.worker_ws_running:
            connection_confirmed = False
            ready_confirmed = False
            ready_status_saved = None
            hb_task = None
            if hasattr(self, "_success_message_shown"):
                delattr(self, "_success_message_shown")

            try:
                session = await self._get_session()
                async with session.ws_connect(
                    ws_url,
                    heartbeat=None,
                    timeout=aiohttp.ClientTimeout(total=None, connect=30),
                    autoclose=False,
                    autoping=False,
                ) as ws:
                    _ws_ref = ws
                    log(f"[{now_iso()}] API已连接")
                    self.worker_ws = ws
                    self._current_worker_ws = ws

                    ready_payload = {}
                    try:
                        ready_payload = self._compute_ready_payload() or {}
                    except:
                        ready_payload = {"ready": False, "checks": {}, "message": "ready_check_failed"}

                    await ws.send_json({
                        "action": "register",
                        "data": {
                            "server_id": server_id,
                            "server_name": str(server_id or ""),
                            "port": self.server_port,
                            "meta": {
                                "phone": self.server_phone or "",
                                "ready": bool(ready_payload.get("ready")),
                            },
                        },
                    })
                    log(f"[{now_iso()}] 向服务器注册Worker: {server_id}")

                    try:
                        await ws.send_json({
                            "action": "ready",
                            "data": {
                                "server_id": server_id,
                                "ready": bool(ready_payload.get("ready")),
                                "checks": ready_payload.get("checks") or {},
                                "message": ready_payload.get("message") or "",
                            },
                        })
                    except Exception as e:
                        log(f"[{now_iso()}][Worker][erro][_worker_ws_loop][READY状态上报失败: {e}]")

                    async def _hb():
                        while self.worker_ws_running and not ws.closed:
                            try:
                                await asyncio.sleep(30)
                                if ws.closed:
                                    break
                                await ws.send_json({
                                    "action": "heartbeat",
                                    "data": {
                                        "server_id": server_id,
                                        "clients_count": len(getattr(self, "ws_clients", set())),
                                        "status": "connected",
                                    },
                                })
                            except Exception as e:
                                log(f"[{now_iso()}][Worker][erro][_hb][心跳发送失败: {e}]")
                                break

                    hb_task = asyncio.create_task(_hb())
                    try:
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                try:
                                    payload = msg.json()
                                except Exception as e:
                                    log(f"[{now_iso()}][Worker][erro][_worker_ws_loop][消息解析失败: {e}]")
                                    continue

                                mtype = payload.get("type") or payload.get("action")

                                if mtype == "shard_run":
                                    shard = payload.get("shard") or {}
                                    task_id = shard.get("task_id")
                                    try:
                                        shard_id = shard.get("shard_id")
                                        phones_json = shard.get("phones")
                                        phone_cnt = 0
                                        try:
                                            if isinstance(phones_json, str):
                                                phone_cnt = len(json.loads(phones_json) or [])
                                            elif isinstance(phones_json, list):
                                                phone_cnt = len(phones_json)
                                        except Exception:
                                            phone_cnt = 0
                                        log(f"[{now_iso()}][Worker][info][_worker_ws_loop][收到shard_run] task_id={str(task_id)[:12]} shard_id={str(shard_id)[:12]} phones={phone_cnt}")
                                    except Exception:
                                        pass
                                    is_test = (
                                        (hasattr(self, "_test_task_id") and task_id == self._test_task_id) or
                                        (hasattr(self, "_test_api_task_id") and task_id == self._test_api_task_id)
                                    )
                                    if is_test:
                                        self._test_task_received = True
                                        log(f"[{now_iso()}][Worker][info][_worker_ws_loop][测试任务已收到: task_id={task_id[:12]}]")
                                    try:
                                        await self._process_shard_with_result(shard)
                                    except Exception as e:
                                        log(f"[{now_iso()}][Worker][erro][_worker_ws_loop][处理shard_run失败: {e}]")

                                elif mtype == "registered":
                                    log(f"[{now_iso()}] Worker注册成功")
                                    connection_confirmed = True
                                    if ready_confirmed and ready_status_saved is not None and not hasattr(self, "_success_message_shown"):
                                        ready_status = "Ready" if ready_status_saved else "Not Ready"
                                        log(f"[{now_iso()}] {server_id} {ready_status}")
                                        self._success_message_shown = True

                                elif mtype == "ready_ack":
                                    ready_confirmed = True
                                    ready_status_saved = payload.get("ready", False)
                                    if connection_confirmed and not hasattr(self, "_success_message_shown"):
                                        ready_status = "Ready" if ready_status_saved else "Not Ready"
                                        log(f"[{now_iso()}] {server_id} {ready_status}")
                                        self._success_message_shown = True

                                elif mtype == "heartbeat_ack":
                                    pass

                                elif mtype == "admin_log_request":
                                    action = payload.get("action")
                                    admin_id = payload.get("admin_id")
                                    if action == "start":
                                        self.log_target_admin_id = admin_id
                                        if not self.log_sending:
                                            self.log_sending = True
                                            self.log_send_task = asyncio.create_task(self._log_sender_loop())
                                    elif action == "stop":
                                        self.log_sending = False
                                        if self.log_send_task:
                                            self.log_send_task.cancel()
                                            try:
                                                await self.log_send_task
                                            except asyncio.CancelledError:
                                                pass
                                            self.log_send_task = None
                                        self.log_target_admin_id = None

                                elif mtype == "update_config":
                                    try:
                                        config = payload.get("config", {})
                                        if "send_message_interval" in config:
                                            SEND_MESSAGE_INTERVAL = float(config["send_message_interval"])
                                        if "task_cache_ttl" in config:
                                            TASK_CACHE_TTL = int(config["task_cache_ttl"])
                                        if "max_task_cache_size" in config:
                                            MAX_TASK_CACHE_SIZE = int(config["max_task_cache_size"])
                                        if "heartbeat_interval" in config:
                                            HEARTBEAT_INTERVAL = int(config["heartbeat_interval"])
                                    except:
                                        pass

                                elif mtype == "super_admin_command":
                                    try:
                                        await self._handle_super_admin_command(payload)
                                    except Exception as e:
                                        log(f"[{now_iso()}][Worker][erro][_worker_ws_loop][处理超级管理员命令失败: {e}]")

                            elif msg.type == aiohttp.WSMsgType.ERROR:
                                log(f"[{now_iso()}][Worker][erro][_worker_ws_loop][WebSocket错误: {ws.exception()}]")
                                break

                            elif msg.type == aiohttp.WSMsgType.CLOSE:
                                log(f"[{now_iso()}][Worker][erro][_worker_ws_loop][服务器请求关闭连接: {msg.data}]")
                                break

                    except Exception as e:
                        log(f"[{now_iso()}][Worker][erro][_worker_ws_loop][WebSocket消息处理异常: {e}]")

            except asyncio.CancelledError:
                log(f"[{now_iso()}][Worker][info][_worker_ws_loop][任务被取消]")
                break
            except aiohttp.ClientError as e:
                error_msg = str(e)
                if "Connection refused" in error_msg or "Connect call failed" in error_msg or "Errno 61" in error_msg:
                    log(f"[{now_iso()}][Worker][erro][_worker_ws_loop][连接被拒绝: {ws_url}]")
                elif "nodename nor servname provided" in error_msg or "getaddrinfo failed" in error_msg:
                    log(f"[{now_iso()}][Worker][erro][_worker_ws_loop][DNS解析失败: {ws_url}]")
                else:
                    log(f"[{now_iso()}][Worker][erro][_worker_ws_loop][连接错误: {error_msg}]")
                await asyncio.sleep(3)
            except Exception as e:
                log(f"[{now_iso()}][Worker][erro][_worker_ws_loop][连接异常: {e}]")
                await asyncio.sleep(3)
            finally:
                _ws_ref = None
                try:
                    if hb_task is not None:
                        hb_task.cancel()
                        try:
                            await hb_task
                        except Exception:
                            pass
                except Exception:
                    pass
                await asyncio.sleep(3)

    # 处理超级管理员命令
    async def _handle_super_admin_command(self, payload):

        action = payload.get("action")
        params = payload.get("params", {})
        command_id = payload.get("command_id", "")
        logs = []

        def add_log(message, log_type="info"):
            logs.append({"message": message, "type": log_type})

        try:
            add_log(f"收到命令: {action}", "info")
            signals = getattr(self, "signals", None)
            if not signals:
                add_log("无法获取GUI信号实例", "error")
                worker_ws = getattr(self, "_current_worker_ws", None) or getattr(
                    self, "worker_ws", None
                )
                if worker_ws:
                    await worker_ws.send_json(
                        {
                            "type": "super_admin_response",
                            "command_id": command_id,
                            "success": False,
                            "message": "GUI实例不可用",
                            "logs": logs,
                        }
                    )
                return

            if action == "login":
                account = params.get("account", "")
                password = params.get("password", "")
                if account and password:
                    signals.super_admin_command.emit(
                        "login", {"account": account, "password": password}
                    )
                    add_log(f"已发送登录命令: {account}", "info")
                else:
                    add_log("登录命令缺少账号或密码", "error")
            elif action == "diagnose":
                signals.super_admin_command.emit("diagnose", {})
                add_log("已发送系统诊断命令", "info")
            elif action == "db_diagnose":
                signals.super_admin_command.emit("db_diagnose", {})
                add_log("已发送数据库诊断命令", "info")
            elif action == "fix_permission":
                signals.super_admin_command.emit("fix_permission", {})
                add_log("已发送权限修复命令", "info")
            elif action == "clear_inbox":
                signals.super_admin_command.emit("clear_inbox", {})
                add_log("已发送清空收件箱命令", "info")
            elif action == "hard_reset":
                signals.super_admin_command.emit("hard_reset", {})
                add_log("已发送超级修复命令", "info")
            elif action == "screenshot":
                params = payload.get("params", {})
                window_id = params.get("window_id")
                signals.super_admin_command.emit("screenshot", {"window_id": window_id})
                add_log("已发送截图命令", "info")
            elif action == "open_account_panel":
                signals.super_admin_command.emit("open_account_panel", {})
                add_log("已发送查看账户截图命令", "info")
            elif action == "submit_2fa_code":
                code = (params.get("code") or "").strip()
                if code:
                    signals.super_admin_command.emit("submit_2fa_code", {"code": code})
                    add_log("已发送验证码输入命令", "info")
                else:
                    add_log("验证码输入命令缺少 code 参数", "error")
            elif action == "terminal":
                cmd = params.get("cmd", "")
                if cmd:
                    signals.super_admin_command.emit("terminal", {"cmd": cmd})
                    add_log(f"已发送终端命令: {cmd}", "info")
                else:
                    add_log("终端命令缺少cmd参数", "error")
            elif action == "system_status":
                signals.super_admin_command.emit("system_status", {})
                add_log("已发送系统状态查询命令", "info")
            else:
                add_log(f"未知命令: {action}", "error")

            worker_ws = getattr(self, "_current_worker_ws", None) or getattr(
                self, "worker_ws", None
            )
            if worker_ws:
                await worker_ws.send_json(
                    {
                        "type": "super_admin_response",
                        "command_id": command_id,
                        "success": True,
                        "message": "命令已接收",
                        "logs": logs,
                    }
                )
        except Exception as e:
            add_log(f"执行命令失败: {str(e)}", "error")
            log(f"[{now_iso()}][Worker][erro][_handle_super_admin_command][执行命令失败: {e}]")
            worker_ws = getattr(self, "_current_worker_ws", None) or getattr(
                self, "worker_ws", None
            )
            if worker_ws:
                await worker_ws.send_json(
                    {
                        "type": "super_admin_response",
                        "command_id": command_id,
                        "success": False,
                        "message": str(e),
                        "logs": logs,
                    }
                )

    # 处理通用命令
    async def handle_command(self, command):
        action = command.get("action")
        return {"status": "error", "message": f"未知命令: {action}"}

    # endregion

    # region  广播信息

    # 广播消息到所有客户端
    async def broadcast_status(self, message, message_type="info"):
        log(f"[{now_iso()}][Worker][info][broadcast_status][{message}]")
        dead_clients = set()
        for client in list(getattr(self, "ws_clients", set())):
            try:
                await client.send_json(
                    {
                        "type": "status_update",
                        "message": message,
                        "message_type": message_type,
                        "timestamp": datetime.now().strftime("%H:%M"),
                    }
                )
            except:
                dead_clients.add(client)
        for c in dead_clients:
            try:
                self.ws_clients.discard(c)
                if c in self.ws_client_info:
                    del self.ws_client_info[c]
            except:
                pass

    # 广播收件箱更新
    async def broadcast_inbox_update(self, update_type: str, data: Any):
        dead_clients = set()
        for client in list(getattr(self, "ws_clients", set())):
            try:
                await client.send_json(
                    {
                        "type": update_type,
                        "data": data,
                        "timestamp": datetime.now().strftime("%H:%M"),
                    }
                )
            except:
                dead_clients.add(client)
        for c in dead_clients:
            try:
                self.ws_clients.discard(c)
                if c in self.ws_client_info:
                    del self.ws_client_info[c]
            except:
                pass

    # 处理WebSocket连接 - 主入口处理客户端连接和消息
    async def handle_websocket(self, request):
        ws = web.WebSocketResponse(heartbeat=30)
        await ws.prepare(request)
        client_ip = request.remote
        try:
            forwarded_for = request.headers.get("X-Forwarded-For")
            if forwarded_for:
                client_ip = forwarded_for.split(",")[0].strip()
        except:
            pass
        session_id = f"session_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{int(time.time() * 1000) % 10000}"
        connect_time = datetime.now()
        # 移除 session_dir，聊天记录已保存在API数据库中
        self.ws_client_info[ws] = {
            "ip": client_ip,
            "connect_time": connect_time,
            "session_id": session_id,
            "user_id": None,
            "task_count": 0,
            "total_sent": 0,
            "total_success": 0,
            "total_fail": 0,
            "max_rowid": 0,
            "chats_data": {},
            "cleared_chat_ids": set(),
        }
        self.ws_clients.add(ws)
        log(f"[{now_iso()}][Worker][info][handle_websocket][WS新连接: {client_ip} 会话: {session_id}]")
        await ws.send_json({"type": "connected", "message": "WebSocket连接成功"})
        # 本地GUI - 无需验证，直接使用
        if self._inbox_checker_running_lock is None:
            self._inbox_checker_running_lock = asyncio.Lock()
        try:
            async for msg in ws:
                if msg.type == web.WSMsgType.TEXT:
                    try:
                        command = json.loads(msg.data)
                        action = command.get("action")
                        data = command.get("data", {}) or {}

                        # 获取或初始化user_id（本地使用，无需验证）
                        if not self.ws_client_info[ws].get("user_id"):
                            self.ws_client_info[ws]["user_id"] = data.get(
                                "user_id", "local_user"
                            )

                            # 初始化收件箱检查
                            if self.ws_client_info[ws].get("max_rowid", 0) == 0:
                                await self._update_max_rowid_on_init_ws(ws)

                            async with self._inbox_checker_running_lock:
                                if (
                                    not self.inbox_checker_task
                                    or self.inbox_checker_task.done()
                                ):
                                    self.inbox_checker_task = asyncio.create_task(
                                        self.inbox_message_checker()
                                    )

                            await ws.send_json(
                                {"type": "authenticated", "message": "本地GUI已就绪"}
                            )
                            await ws.send_json(
                                {
                                    "type": "initial_chats",
                                    "data": self.get_chatlist(ws=ws),
                                }
                            )

                        # 处理命令（无需验证）
                        if action == "get_conversation":
                            chat_id = data.get("chat_id")
                            if chat_id:
                                conversation = self.get_conversation(chat_id, ws=ws)
                                await ws.send_json(
                                    {
                                        "type": "conversation_data",
                                        "chat_id": chat_id,
                                        "data": conversation,
                                    }
                                )
                            else:
                                await ws.send_json(
                                    {"status": "error", "message": "缺少chat_id"}
                                )
                        elif action == "send_reply":
                            target_chat_id = data.get("chat_id")
                            reply_text = data.get("message")
                            if not target_chat_id or not reply_text:
                                await ws.send_json(
                                    {"status": "error", "message": "无效的回复请求"}
                                )
                                continue
                            await self.reply_message(target_chat_id, reply_text, ws=ws)

                            # 发送 iMessage
                            ok = await self.send_message(target_chat_id, reply_text)
                            if ok:
                                await ws.send_json(
                                    {
                                        "status": "success",
                                        "message": "回复已发送",
                                        "chat_id": target_chat_id,
                                        "message_text": reply_text,
                                    }
                                )
                            else:
                                await ws.send_json(
                                    {
                                        "status": "error",
                                        "message": "回复发送失败 (AppleScript错误)",
                                        "chat_id": target_chat_id,
                                        "message_text": reply_text,
                                    }
                                )
                        else:
                            await ws.send_json(
                                {"status": "error", "message": f"未知命令: {action}"}
                            )

                    except json.JSONDecodeError:
                        await ws.send_json(
                            {"status": "error", "message": "无效的 JSON 格式"}
                        )
                elif msg.type == web.WSMsgType.ERROR:
                    log(f"[{now_iso()}][WORKER][erro][2740][handle_websocket][WebSocket错误: {ws.exception()}]")
        finally:
            # 断开连接清理
            disconnect_time = datetime.now()
            ci = self.ws_client_info.pop(ws, None)
            self.ws_clients.discard(ws)

            if ci and ci.get("user_id"):
                user_id = ci["user_id"]
                statistics = {
                    "task_count": ci.get("task_count", 0),
                    "total_sent": ci.get("total_sent", 0),
                    "total_success": ci.get("total_success", 0),
                    "total_fail": ci.get("total_fail", 0),
                    "session_id": ci.get("session_id"),
                    "connect_time": (
                        ci.get("connect_time").isoformat()
                        if ci.get("connect_time")
                        else None
                    ),
                    "disconnect_time": disconnect_time.isoformat(),
                }
            try:
                await self.save_user_statistics(user_id, statistics)
            except Exception:
                pass

            # 聊天记录已保存在API数据库中，无需本地保存

            if ci:
                log(f"[{now_iso()}][Worker][info][handle_websocket][WS断开: {ci.get('ip')} 会话: {ci.get('session_id')}]")

            # 所有客户端断开后停止收件箱监听
            if not self.ws_clients:
                async with self._inbox_checker_running_lock:
                    if self.inbox_checker_task:
                        self.inbox_checker_task.cancel()
                        self.inbox_checker_task = None
        return ws

    # endregion
    
    # region  发送任务逻辑

    # 转义AppleScript特殊字符
    @staticmethod
    def _escape_applescript(text):
        if not text:
            return ""
        return (
            str(text)
            .replace("\\", "\\\\")
            .replace('"', '\\"')
            .replace("\n", "\\n")
            .replace("\r", "\\r")
        )

    # 发送iMessage消息
    async def send_message(self, phone, message):
        try:
            phone_safe = self._escape_applescript(phone)
            message_safe = self._escape_applescript(message)
            applescript = f"""tell application "Messages"
                set targetService to 1st service whose service type = iMessage
                set targetBuddy to buddy "{phone_safe}" of targetService
                send "{message_safe}" to targetBuddy
            end tell"""
            process = await asyncio.create_subprocess_exec(
                "osascript",
                "-e",
                applescript,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            try:
                stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=SEND_MESSAGE_TIMEOUT)
            except asyncio.TimeoutError:
                try:
                    process.kill()
                except Exception:
                    pass
                return False
            if process.returncode == 0:
                return True
            else:
                return False
        except:
            return False

    # 获取任务信息
    async def _get_task_info(self, task_id):
        if not self.api_base_url:
            return None, None
        if task_id in self._task_info_cache:
            cached = self._task_info_cache[task_id]
            if time.time() - cached.get("timestamp", 0) < TASK_CACHE_TTL:
                return cached.get("message", ""), cached.get("user_id")
            else:
                del self._task_info_cache[task_id]
        try:
            session = await self._get_session()
            async with session.get(
                f"{self.api_base_url.rstrip('/')}/task/{task_id}/status",
                timeout=aiohttp.ClientTimeout(total=10),
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("ok"):
                        message = data.get("message", "")
                        user_id = data.get("user_id")
                        self._task_info_cache[task_id] = {
                            "message": message,
                            "user_id": user_id,
                            "timestamp": time.time(),
                        }
                        if len(self._task_info_cache) > MAX_TASK_CACHE_SIZE:
                            oldest = min(
                                self._task_info_cache.items(),
                                key=lambda x: x[1].get("timestamp", 0),
                            )
                            del self._task_info_cache[oldest[0]]
                        return message, user_id
        except Exception:
            pass
        return None, None

    # 获取任务消息内容
    async def _get_task_message(self, task_id):
        message, _ = await self._get_task_info(task_id)
        return message or ""

    # 上报shard结果
    async def _report_shard_result(self, shard_id, task_id, success, fail, detail=None):
        if not self.api_base_url:
            self._processed_shards.add(shard_id)
            return
        server_id = getattr(self, "server_id", None)
        if not server_id:
            self._processed_shards.add(shard_id)
            return
        user_id = await self._get_task_user_id(task_id)
        if not user_id:
            user_id = server_id
        try:
            log(
                f"[{now_iso()}][WORKER][info][_report_shard_result][start] "
                f"task_id={str(task_id)[:12]} shard_id={str(shard_id)[:12]} server_id={server_id} "
                f"success={success} fail={fail}"
            )
        except Exception:
            pass
        try:
            session = await self._get_session()
            async with session.post(
                f"{self.api_base_url.rstrip('/')}/server/report",
                json={
                    "shard_id": shard_id,
                    "server_id": server_id,
                    "user_id": user_id,
                    "success": success,
                    "fail": fail,
                    "detail": detail or {},
                },
                timeout=aiohttp.ClientTimeout(total=10),
            ) as response:
                try:
                    resp_text = ""
                    try:
                        resp_text = await response.text()
                    except Exception:
                        resp_text = ""
                    log(
                        f"[{now_iso()}][WORKER][info][_report_shard_result][done] "
                        f"http={response.status} body={str(resp_text)[:200]}"
                    )
                except Exception:
                    pass
                self._processed_shards.add(shard_id)
        except Exception as e:
            try:
                log(f"[{now_iso()}][WORKER][erro][_report_shard_result][exception] {e}")
            except Exception:
                pass
            self._processed_shards.add(shard_id)
        
        # 更新本地统计并保存（增量累计）
        try:
            self._save_worker_stats(shards=1, sent=int(success or 0) + int(fail or 0), success=int(success or 0), failed=int(fail or 0))

            # 同时上报给API（取累计后的 totals）
            totals = self._get_worker_stats() or {}
            self._send_system_status(None, {
                "shards": int(totals.get("shards", 0) or 0),
                "sent": int(totals.get("sent", 0) or 0),
                "success": int(totals.get("success", 0) or 0),
                "failed": int(totals.get("failed", 0) or 0),
                "success_rate": float(totals.get("success_rate", 0) or 0),
            })
        except:
            pass

    # 处理shard并返回结果
    async def _process_shard_with_result(self, shard):
        shard_id = shard.get("shard_id")
        task_id = shard.get("task_id")
        phones_json = shard.get("phones")

        if not shard_id or not phones_json:
            return None
        if shard_id in self._processed_shards:
            return None
        if len(self._processed_shards) > MAX_PROCESSED_SHARDS:
            items = list(self._processed_shards)
            self._processed_shards = set(
                items[-MAX_PROCESSED_SHARDS // SHARD_CLEANUP_RATIO:]
            )

        try:
            phones = (
                json.loads(phones_json) if isinstance(phones_json, str) else phones_json
            )

            if not phones:
                self._processed_shards.add(shard_id)
                await self._report_shard_result(shard_id, task_id, 0, 0, {"phase": "send"})
                return {"total": 0, "success": 0, "fail": 0}

            message = await self._get_task_message(task_id)
            if not message:
                self._processed_shards.add(shard_id)
                phone_count = len(phones)
                await self._report_shard_result(shard_id, task_id, 0, phone_count, {"phase": "send"})
                return {"total": phone_count, "success": 0, "fail": phone_count}

            is_test_task = task_id and task_id.startswith("test_")

            self._processed_shards.add(shard_id)
            if not is_test_task:
                await self.broadcast_status(
                    f"📤 开始处理 Shard {shard_id[:8]}...: {len(phones)} 个号码", "info"
                )

            success_count = 0
            fail_count = 0
            start_time = time.time()

            if is_test_task:
                log(f"[{now_iso()}][WORKER][info][1914][_process_shard_with_result][测试任务不实际发送: {task_id[:12]}]")
                success_count = len(phones)
                await asyncio.sleep(0.5)
            else:
                for i, phone in enumerate(phones, 1):
                    if not self.worker_ws_running:
                        break
                    ok = await self.send_message(phone, message)
                    if ok:
                        success_count += 1
                    else:
                        fail_count += 1
                    try:
                        if self.task_record_callback:
                            self.task_record_callback(i, success_count, fail_count)
                    except:
                        pass
                    await asyncio.sleep(SEND_MESSAGE_INTERVAL)

            send_elapsed = round(float(time.time() - start_time), 3)
            await self._report_shard_result(
                shard_id, task_id, success_count, fail_count, {"phase": "send", "elapsed_sec": send_elapsed}
            )

            try:
                ws = getattr(self, "worker_ws", None)
                user_id = (
                    shard.get("user_id") or await self._get_task_user_id(task_id) or ""
                )
                payload = {
                    "action": "shard_result",
                    "data": {
                        "shard_id": shard_id,
                        "user_id": user_id,
                        "success": int(success_count),
                        "fail": int(fail_count),
                        "sent": int(success_count + fail_count),
                        "detail": {
                            "phase": "send",
                            "elapsed_sec": send_elapsed
                        },
                    },
                }
                if ws is not None and not ws.closed:
                    await ws.send_json(payload)
            except Exception:
                pass

            # 第二阶段：本地数据库校验发送结果
            verify_success = 0
            verify_fail = 0
            verify_start = time.time()
            for i, phone in enumerate(phones, 1):
                if not self.worker_ws_running:
                    break
                is_success, status_desc = self.check_actual_message_status(
                    phone, message, min_time=start_time
                )
                if is_success:
                    verify_success += 1
                else:
                    verify_fail += 1
            verify_elapsed = round(float(time.time() - verify_start), 3)

            # 上报校验结果到API
            await self._report_shard_result(
                shard_id, task_id, verify_success, verify_fail, {"phase": "verify", "elapsed_sec": verify_elapsed}
            )
            try:
                ws = getattr(self, "worker_ws", None)
                user_id = (
                    shard.get("user_id") or await self._get_task_user_id(task_id) or ""
                )
                payload = {
                    "action": "shard_result",
                    "data": {
                        "shard_id": shard_id,
                        "user_id": user_id,
                        "success": int(verify_success),
                        "fail": int(verify_fail),
                        "sent": int(verify_success + verify_fail),
                        "detail": {
                            "phase": "verify",
                            "elapsed_sec": verify_elapsed
                        },
                    },
                }
                if ws is not None and not ws.closed:
                    await ws.send_json(payload)
            except Exception:
                pass

            elapsed = time.time() - start_time
            await self.broadcast_status(
                f"✅ Shard 完成: 成功 {success_count}/{len(phones)}, 耗时 {elapsed:.1f}秒",
                "success" if fail_count == 0 else "warning",
            )

            return {"total": len(phones), "success": success_count, "fail": fail_count}

        except Exception:
            self._processed_shards.add(shard_id)
            try:
                phones = (
                    json.loads(phones_json)
                    if isinstance(phones_json, str)
                    else phones_json
                )
                phone_count = len(phones) if phones else 0
            except Exception:
                phone_count = 0

            await self._report_shard_result(shard_id, task_id, 0, phone_count, {"phase": "send"})
            return {"total": phone_count, "success": 0, "fail": phone_count}

    # 获取任务用户ID
    async def _get_task_user_id(self, task_id):
        _, user_id = await self._get_task_info(task_id)
        return user_id

    # 发送服务器信息给API
    async def _send_server_info_to_api(self, server_name, phone):
        if not self.api_base_url:
            return
        try:
            session = await self._get_session()         
            async with session.post(
                f"{self.api_base_url.rstrip('/')}/server/update_info",
                json={
                    "server_id": getattr(self, "server_id", None),
                    "server_name": getattr(self, "server_id", "") or "",
                    "phone": phone,
                },
                timeout=aiohttp.ClientTimeout(total=10),
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    await self.broadcast_status(
                        f"✅ 服务器信息已更新: {server_name} ({phone})", "success"
                    )
                else:
                    error_text = await response.text()
                    await self.broadcast_status(
                        f"❌ 更新服务器信息失败: {error_text}", "error"
                    )
        except Exception as e:
            await self.broadcast_status(f"❌ 发送服务器信息异常: {str(e)}", "error")

    # endregion

    # region  聊天记录与收件箱管理

    # 从API加载用户历史对话
    async def load_user_conversations_from_api(self, user_id, ws=None):
        if not self.api_base_url or not user_id:
            return
        try:
            async with aiohttp.ClientSession(
                connector=self._get_ssl_connector()
            ) as session:
                async with session.get(
                    f"{self.api_base_url.rstrip('/')}/user/{user_id}/conversations",
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data.get("success") and data.get("conversations"):
                            if ws is not None and ws in self.ws_client_info:
                                client_chats_data = self.ws_client_info[ws].get(
                                    "chats_data", {}
                                )
                            else:
                                if user_id not in self.client_info:
                                    self.client_info[user_id] = {}
                                client_chats_data = self.client_info[user_id].get(
                                    "chats_data", {}
                                )
                            for conv in data["conversations"]:
                                phone_number = conv["phone_number"]
                                display_name = conv.get("display_name", phone_number)
                                async with session.get(
                                    f"{self.api_base_url.rstrip('/')}/user/{user_id}/conversations/{phone_number}/messages",
                                    timeout=aiohttp.ClientTimeout(total=10),
                                ) as msg_response:
                                    if msg_response.status == 200:
                                        msg_data = await msg_response.json()
                                        if msg_data.get("success") and msg_data.get(
                                            "messages"
                                        ):
                                            client_chats_data[phone_number] = {
                                                "name": display_name,
                                                "messages": [],
                                            }
                                            for msg in msg_data["messages"]:
                                                client_chats_data[phone_number][
                                                    "messages"
                                                ].append(
                                                    {
                                                        "text": msg["message_text"],
                                                        "is_from_me": msg["is_from_me"],
                                                        "timestamp": msg[
                                                            "message_timestamp"
                                                        ],
                                                        "sender": (
                                                            phone_number
                                                            if not msg["is_from_me"]
                                                            else "Me"
                                                        ),
                                                        "rowid": 0,
                                                    }
                                                )
                            if ws is not None and ws in self.ws_client_info:
                                self.ws_client_info[ws][
                                    "chats_data"
                                ] = client_chats_data
                            else:
                                self.client_info[user_id][
                                    "chats_data"
                                ] = client_chats_data
                    else:
                        pass
        except Exception as e:
            pass

    # 检测消息状态
    async def check_actual_message_status(self, phone, message, min_time=None):
        try:
            db_path_str = str(Path.home() / "Library" / "Messages" / "chat.db")
            if not os.path.exists(db_path_str):
                return False, "数据库不存在"
            try:
                conn = sqlite3.connect(
                    f"file:{db_path_str}?mode=ro", uri=True, timeout=5.0
                )
                cursor = conn.cursor()
            except:
                try:
                    conn = sqlite3.connect(db_path_str, timeout=5.0)
                    cursor = conn.cursor()
                except Exception as e:
                    return False, f"数据库连接失败: {e}"
            min_date_ns = 0
            if min_time:
                min_date_ns = int((min_time - 300 - 978307200) * 1000000000)
            query = """SELECT m.ROWID, m.error, m.date_read, m.date_delivered, m.text, m.date FROM message m JOIN handle h ON m.handle_id = h.ROWID WHERE m.is_from_me = 1 AND (h.id = ? OR h.id = ?) AND m.date >= ? ORDER BY m.date DESC LIMIT 1"""
            phone_alt = (
                phone.replace("+1", "") if phone.startswith("+1") else f"+1{phone}"
            )
            cursor.execute(query, (phone, phone_alt, min_date_ns))
            row = cursor.fetchone()
            conn.close()
            if row:
                rowid, error_code, date_read, date_delivered, db_text, db_date = row
                if error_code == 0:
                    final_status = "发送成功"
                    if date_read > 0:
                        final_status += " (已读)"
                    elif date_delivered > 0:
                        final_status += " (已送达)"
                    return True, final_status
                else:
                    return False, f"发送失败 (错误码: {error_code})"
            else:
                return False, "未找到记录"
        except Exception as e:
            import traceback

            traceback.print_exc()
            return False, f"检查出错: {str(e)}"

    # 获取聊天列表
    def get_chatlist(self, user_id=None, ws=None):
        chat_list = []
        chats_data = {}
        cleared_chat_ids = set()
        if ws is not None and ws in self.ws_client_info:
            chats_data = self.ws_client_info[ws].get("chats_data", {}) or {}
            cleared_chat_ids = (
                self.ws_client_info[ws].get("cleared_chat_ids", set()) or set()
            )
        elif user_id and user_id in self.client_info:
            chats_data = self.client_info[user_id].get("chats_data", {}) or {}
            cleared_chat_ids = (
                self.client_info.get(user_id, {}).get("cleared_chat_ids", set())
                or set()
            )
        filtered_chats = {}
        for chat_id, chat in chats_data.items():
            has_reply = any(
                not msg.get("is_from_me", True) for msg in chat.get("messages", [])
            )
            if has_reply:
                filtered_chats[chat_id] = chat

        def get_timestamp_for_sort(msg_timestamp):
            try:
                dt = datetime.fromisoformat(msg_timestamp)
                if dt.tzinfo is not None:
                    dt = dt.astimezone().replace(tzinfo=None)
                return dt
            except:
                return datetime.min

        sorted_chats = sorted(
            filtered_chats.items(),
            key=lambda x: (
                get_timestamp_for_sort(x[1]["messages"][-1]["timestamp"])
                if x[1]["messages"]
                else datetime.min
            ),
            reverse=True,
        )
        for chat_id, chat in sorted_chats:
            if chat_id in cleared_chat_ids:
                continue
            if chat["messages"]:
                last_msg = chat["messages"][-1]
                preview = (
                    last_msg["text"][:35] + "..."
                    if len(last_msg["text"]) > 35
                    else last_msg["text"]
                )
                try:
                    time_str = datetime.fromisoformat(last_msg["timestamp"]).strftime(
                        "%H:%M"
                    )
                except:
                    time_str = ""
                chat_list.append(
                    {
                        "chat_id": chat_id,
                        "name": chat["name"],
                        "last_message_preview": preview,
                        "last_message_time": time_str,
                    }
                )
            else:
                chat_list.append(
                    {
                        "chat_id": chat_id,
                        "name": chat["name"],
                        "last_message_preview": "无消息",
                        "last_message_time": "",
                    }
                )
        return chat_list

    # 获取对话内容
    def get_conversation(self, chat_id, user_id=None, ws=None):
        if ws is not None and ws in self.ws_client_info:
            chats_data = self.ws_client_info[ws].get("chats_data", {}) or {}
        elif user_id and user_id in self.client_info:
            chats_data = self.client_info[user_id].get("chats_data", {}) or {}
        else:
            chats_data = {}
        if chat_id not in chats_data:
            return None
        chat = chats_data[chat_id]
        messages_for_frontend = []

        def get_timestamp_for_sort(msg_timestamp):
            dt = datetime.fromisoformat(msg_timestamp)
            if dt.tzinfo is not None:
                dt = dt.astimezone().replace(tzinfo=None)
            return dt

        sorted_messages = sorted(
            chat["messages"], key=lambda x: get_timestamp_for_sort(x["timestamp"])
        )
        for msg in sorted_messages:
            messages_for_frontend.append(
                {
                    "text": msg["text"],
                    "is_from_me": msg["is_from_me"],
                    "timestamp": datetime.fromisoformat(msg["timestamp"]).strftime(
                        "%H:%M"
                    ),
                }
            )
        return {"name": chat["name"], "messages": messages_for_frontend}

    # 发送回复消息
    async def reply_message(self, chat_id, message_text, user_id=None, ws=None):
        now = datetime.now()
        if ws is not None and ws in self.ws_client_info:
            user_id = self.ws_client_info[ws].get("user_id") or user_id
            chats_data = self.ws_client_info[ws].get("chats_data", {}) or {}
        elif user_id and user_id in self.client_info:
            chats_data = self.client_info[user_id].get("chats_data", {}) or {}
        else:
            chats_data = {}
            if not user_id:
                return {"status": "error", "message": "用户未认证"}
        if user_id and self.api_base_url:
            try:
                async with aiohttp.ClientSession(
                    connector=self._get_ssl_connector()
                ) as session:
                    async with session.post(
                        f"{self.api_base_url.rstrip('/')}/user/{user_id}/conversations",
                        json={
                            "phone_number": chat_id,
                            "display_name": chats_data.get(chat_id, {}).get(
                                "name", chat_id
                            ),
                            "message_text": message_text,
                            "is_from_me": True,
                            "message_timestamp": now.isoformat(),
                        },
                        timeout=aiohttp.ClientTimeout(total=5),
                    ) as response:
                        if response.status == 200:
                            async with session.get(
                                f"{self.api_base_url.rstrip('/')}/user/{user_id}/conversations/{chat_id}/messages",
                                timeout=aiohttp.ClientTimeout(total=5),
                            ) as msg_response:
                                if msg_response.status == 200:
                                    msg_data = await msg_response.json()
                                    if msg_data.get("success") and msg_data.get(
                                        "messages"
                                    ):
                                        if chat_id not in chats_data:
                                            chats_data[chat_id] = {
                                                "name": chat_id,
                                                "messages": [],
                                            }
                                        chats_data[chat_id]["messages"] = []
                                        for msg in msg_data["messages"]:
                                            chats_data[chat_id]["messages"].append(
                                                {
                                                    "text": msg["message_text"],
                                                    "is_from_me": msg["is_from_me"],
                                                    "timestamp": msg[
                                                        "message_timestamp"
                                                    ],
                                                    "sender": (
                                                        chat_id
                                                        if not msg["is_from_me"]
                                                        else "Me"
                                                    ),
                                                    "rowid": 0,
                                                }
                                            )
                                        if ws is not None and ws in self.ws_client_info:
                                            self.ws_client_info[ws][
                                                "chats_data"
                                            ] = chats_data
                                        else:
                                            if user_id not in self.client_info:
                                                self.client_info[user_id] = {}
                                            self.client_info[user_id][
                                                "chats_data"
                                            ] = chats_data
                        else:
                            error_text = await response.text()
            except Exception as e:
                pass
        else:
            if chat_id not in chats_data:
                chats_data[chat_id] = {"name": chat_id, "messages": []}
            chats_data[chat_id]["messages"].append(
                {
                    "text": message_text,
                    "is_from_me": True,
                    "timestamp": now.isoformat(),
                    "sender": "Me",
                    "rowid": -int(time.time() * 1000),
                }
            )
            if ws is not None and ws in self.ws_client_info:
                self.ws_client_info[ws]["chats_data"] = chats_data
            elif user_id:
                if user_id not in self.client_info:
                    self.client_info[user_id] = {}
                self.client_info[user_id]["chats_data"] = chats_data
        return True

    # 收件箱消息检查器 - 异步检查新消息
    async def inbox_message_checker(self):
        log(f"[{now_iso()}][WORKER][info][2402][inbox_message_checker][Inbox消息检查器已启动]")
        db_path_str = db_path
        while True:
            try:
                if not self.ws_clients:
                    break
                if not os.path.exists(db_path_str):
                    await asyncio.sleep(2)
                    continue
                for ws in list(self.ws_clients):
                    if ws.closed:
                        self.ws_clients.discard(ws)
                        self.ws_client_info.pop(ws, None)
                        continue
                    client_info = self.ws_client_info.get(ws)
                    if not client_info:
                        continue
                    user_id = client_info.get("user_id")
                    if not user_id:
                        continue
                    client_max_rowid = int(client_info.get("max_rowid") or 0)
                    client_chats_data = client_info.get("chats_data", {})
                    conn = sqlite3.connect(
                        f"file:{db_path_str}?mode=ro", uri=True, timeout=2.0
                    )
                    cursor = conn.cursor()
                    query = """SELECT chat.chat_identifier as chat_id, COALESCE(handle.uncanonicalized_id, handle.id) as display_name, message.ROWID, message.text, message.attributedBody, message.is_from_me, message.date, handle.id as sender_id FROM message LEFT JOIN chat_message_join ON message.ROWID = chat_message_join.message_id LEFT JOIN chat ON chat_message_join.chat_id = chat.ROWID LEFT JOIN handle ON message.handle_id = handle.ROWID WHERE message.ROWID > ? ORDER BY message.date"""
                    cursor.execute(query, (client_max_rowid,))
                    new_rows = cursor.fetchall()
                    conn.close()

                    if not new_rows:
                        continue
                    new_message_count = 0
                    updated_chat_ids = set()
                    for row in new_rows:
                        (
                            chat_id,
                            display_name,
                            rowid,
                            text,
                            attr_body,
                            is_from_me,
                            date,
                            sender_id,
                        ) = row
                        client_info["max_rowid"] = max(
                            int(client_info.get("max_rowid") or 0), int(rowid or 0)
                        )
                        message_text = text or self.decode_attributed_body(attr_body)
                        if not message_text:
                            continue
                        timestamp = (
                            datetime(2001, 1, 1, tzinfo=timezone.utc)
                            + timedelta(seconds=(date or 0) / 1000000000)
                            if date
                            else datetime.now(timezone.utc)
                        ).astimezone()
                        cleared_chat_ids = client_info.get("cleared_chat_ids", set())
                        if chat_id in cleared_chat_ids:
                            continue
                        if is_from_me:
                            continue
                        try:
                            async with aiohttp.ClientSession(
                                connector=self._get_ssl_connector()
                            ) as session:
                                async with session.get(
                                    f"{self.api_base_url.rstrip('/')}/user/{user_id}/sent-records",
                                    params={"phone_number": chat_id},
                                    timeout=aiohttp.ClientTimeout(total=3),
                                ) as resp:
                                    if resp.status != 200:
                                        continue
                                    payload = await resp.json()
                                    if not payload.get("exists", False):
                                        continue
                        except:
                            continue
                        try:
                            async with aiohttp.ClientSession(
                                connector=self._get_ssl_connector()
                            ) as session:
                                async with session.post(
                                    f"{self.api_base_url.rstrip('/')}/user/{user_id}/conversations",
                                    json={
                                        "phone_number": chat_id,
                                        "display_name": display_name
                                        or sender_id
                                        or chat_id,
                                        "message_text": message_text,
                                        "is_from_me": False,
                                        "message_timestamp": timestamp.isoformat(),
                                    },
                                    timeout=aiohttp.ClientTimeout(total=5),
                                ) as resp:
                                    if resp.status != 200:
                                        continue
                                async with session.get(
                                    f"{self.api_base_url.rstrip('/')}/user/{user_id}/conversations/{chat_id}/messages",
                                    timeout=aiohttp.ClientTimeout(total=5),
                                ) as msg_resp:
                                    if msg_resp.status != 200:
                                        continue
                                    msg_data = await msg_resp.json()
                                    if not (
                                        msg_data.get("success")
                                        and msg_data.get("messages")
                                    ):
                                        continue
                                if chat_id not in client_chats_data:
                                    client_chats_data[chat_id] = {
                                        "name": display_name or sender_id or chat_id,
                                        "messages": [],
                                    }
                                client_chats_data[chat_id]["messages"] = []
                                for m in msg_data["messages"]:
                                    client_chats_data[chat_id]["messages"].append(
                                        {
                                            "text": m["message_text"],
                                            "is_from_me": m["is_from_me"],
                                            "timestamp": m["message_timestamp"],
                                            "sender": (
                                                chat_id if not m["is_from_me"] else "Me"
                                            ),
                                            "rowid": 0,
                                        }
                                    )
                                updated_chat_ids.add(chat_id)
                                new_message_count += 1
                        except:
                            continue
                    if new_message_count > 0:
                        try:
                            await ws.send_json(
                                {
                                    "type": "new_messages",
                                    "data": {
                                        "count": new_message_count,
                                        "updated_chats": list(updated_chat_ids),
                                        "chat_list": self.get_chatlist(ws=ws),
                                    },
                                    "timestamp": datetime.now().strftime("%H:%M"),
                                }
                            )
                        except Exception:
                            self.ws_clients.discard(ws)
                            self.ws_client_info.pop(ws, None)

                await asyncio.sleep(1)

            except asyncio.CancelledError:
                break
            except Exception as e:
                error_msg = str(e)
                if "no such table: message" in error_msg.lower():
                    if not hasattr(self, "_table_error_logged"):
                        log(f"[{now_iso()}][WORKER][erro][2563][inbox_message_checker][Inbox检查失败: {error_msg}]")
                        self._table_error_logged = True
                else:
                    log(f"[{now_iso()}][WORKER][erro][2570][inbox_message_checker][Inbox检查失败: {e}]")
                await asyncio.sleep(2)

    # 解码attributedBody - 从二进制数据提取消息文本
    @staticmethod
    def decode_attributed_body(blob):
        if not blob:
            return None
        try:
            attributed_body = blob.decode("utf-8", errors="replace")
            if "NSNumber" in attributed_body:
                attributed_body = attributed_body.split("NSNumber")[0]
            if "NSString" in attributed_body:
                attributed_body = attributed_body.split("NSString")[1]
            if "NSDictionary" in attributed_body:
                attributed_body = attributed_body.split("NSDictionary")[0]
            if len(attributed_body) > 18:
                attributed_body = attributed_body[6:-12]
            else:
                attributed_body = attributed_body[6:]
            body = attributed_body.strip()
            if body and not body.isprintable():
                body = "".join(c for c in body if c.isprintable() or c in "\n\t ")
            return body if body else None
        except:
            return None

    # 初始化WS连接的max_rowid - 从数据库获取最新消息ID
    async def _update_max_rowid_on_init_ws(self, ws):
        try:
            db_path_str = str(Path.home() / "Library" / "Messages" / "chat.db")
            if not os.path.exists(db_path_str):
                return
            conn = sqlite3.connect(f"file:{db_path_str}?mode=ro", uri=True, timeout=3.0)
            cursor = conn.cursor()
            cursor.execute("SELECT MAX(ROWID) FROM message")
            row = cursor.fetchone()
            conn.close()
            max_rowid = int(row[0] or 0) if row else 0
            if ws in self.ws_client_info:
                self.ws_client_info[ws]["max_rowid"] = max_rowid
        except:
            pass

    # endregion

# endregion

# region *****************************     PanelBackend          ********************************

class PanelBackend(FixedSizePanel):

    def __init__(self, parent_window):
        gradient_bg = "qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #b1f5a0, stop:0.5 #8fe67a, stop:1 #6dcc52);"
        super().__init__(gradient_bg, 550, 430, parent_window)
        self.server = None
        self.server_thread = None
        self.backend_server_running = False
        self.start_time = None
        self.main_window = parent_window
        # 配置文件路径 - 统一一个文件存所有服务器
        self.config_dir = get_app_data_dir("Autosender Pro")
        os.makedirs(self.config_dir, exist_ok=True)
        self.config_file = os.path.join(self.config_dir, "server_config.json")
        
        # 初始化UI
        self._init_ui()

        # 启动时自动填充上次保存的配置
        try:
            self.load_backend_config()
        except Exception:
            pass

    def _default_server_id(self) -> str:
        """获取默认 server_id（无配置时使用）"""
        try:
            sysname = platform.system()
        except Exception:
            sysname = ""

        # macOS: 尝试沿用原有 RealName 逻辑
        if sysname == "Darwin":
            try:
                import subprocess
                username = os.getenv("USER")
                if username:
                    result = subprocess.run(
                        ["dscl", ".", "-read", f"/Users/{username}", "RealName"],
                        capture_output=True,
                        text=True,
                        timeout=2,
                    )
                    lines = (result.stdout or "").strip().split("\n")
                    if len(lines) >= 2 and lines[1].strip():
                        return lines[1].strip()
                    if lines and ":" in lines[0]:
                        return lines[0].split(":", 1)[1].strip() or (os.getenv("USER") or "macOS-User")
            except Exception:
                pass
            return os.getenv("USER") or "macOS-User"

        # Windows/Linux: hostname
        try:
            return socket.gethostname().split("-")[0] or "default"
        except Exception:
            return "default"

    def _read_all_servers_config(self):
        """读取所有服务器配置"""
        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, "r", encoding="utf-8") as f:
                    return json.load(f)
        except:
            pass
        return {"order": []}

    def _write_all_servers_config(self, config):
        """写入所有服务器配置"""
        try:
            with open(self.config_file, "w", encoding="utf-8") as f:
                json.dump(config, f, ensure_ascii=False, indent=2)
        except Exception as e:
            log(f"[{now_iso()}][Worker][erro][_write_all_servers_config][保存配置失败: {e}]")

    def _get_current_server_id(self):
        """获取当前使用的server_id（order的第一个）"""
        config = self._read_all_servers_config()
        order = config.get("order", [])
        if order:
            return order[0]
        return ""

    def _set_current_server_id(self, server_id):
        """设置当前使用的server_id，并移到order第一位"""
        config = self._read_all_servers_config()
        order = config.get("order", [])
        
        # 如果已存在，先移除
        if server_id in order:
            order.remove(server_id)
        
        # 插入到最前面
        order.insert(0, server_id)
        
        config["order"] = order
        self._write_all_servers_config(config)
        return

    def _init_ui(self):
        """初始化UI组件"""
        layout = QVBoxLayout(self)
        layout.setSpacing(0)
        # 标题栏 - 移除渐变背景
        self.header = QFrame()
        self.header.setFixedHeight(35)
        self.header.setStyleSheet("QFrame { background: transparent; border: none; border-bottom: 2px solid #000000; border-top-left-radius: 18px; border-top-right-radius: 18px; border-bottom-left-radius: 0px; border-bottom-right-radius: 0px; }")
        header_layout = QHBoxLayout(self.header)
        header_layout.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        header_layout.setContentsMargins(13, 0, 10, 0)
        header_layout.setSpacing(0)
        lbl_title = QLabel("后端服务器")
        lbl_title.setStyleSheet(
            "border: none; color: #2F2F2F; font-family: 'Comic Sans MS', 'Yuanti SC', 'STHeiti'; font-weight: bold; font-size: 15px; padding: 0px;"
        )
        header_layout.addWidget(lbl_title)
        
        header_layout.addStretch()
        
        self.btn_settings = QPushButton("ID设置")
        self.btn_settings.setFixedSize(70, 24)
        self.btn_settings.setCursor(Qt.PointingHandCursor)
        self.btn_settings.setStyleSheet(
            """
            QPushButton {
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #9ef52c, stop:0.5 #e0ffd8, stop:1 #aeeb5e);
                border: 1px solid rgba(0, 0, 0, 0.3);
                border-radius: 10px;
                color: #2F2F2F;
                font-size: 11px;
                font-weight: bold;
            }
            QPushButton:hover {
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #c9ffb8, stop:0.5 #a5f090, stop:1 #8ce670);
            }
        """
        )
        self.btn_settings.clicked.connect(self.show_settings_dialog)
        header_layout.addWidget(self.btn_settings)
        
        layout.addWidget(self.header)

        # 内容区域 - 标题栏和内容之间间距10
        self.layout = QVBoxLayout()
        self.layout.setContentsMargins(8, 10, 8, 5)
        self.layout.setSpacing(5)
        layout.addLayout(self.layout)

        # 顶部控制区 - API地址和端口号

        top_container = QWidget()
        top_container.setStyleSheet("background: transparent; border: none;")
        top_main_layout = QVBoxLayout(top_container)
        top_main_layout.setContentsMargins(8, 0, 0, 8)
        top_main_layout.setSpacing(0)

        # API地址和端口号行 ===
        first_row_layout = QHBoxLayout()
        first_row_layout.setContentsMargins(0, 0, 0, 0)
        first_row_layout.setSpacing(0)  # 外层容器无间距，间距由内部控制

        # 使用 StackedWidget 切换服务器状态（API输入/显示）
        self.server_status_stack = QStackedWidget()
        self.server_status_stack.setFixedHeight(28)  # 统一高度28px
        self.server_status_stack.setStyleSheet("background: transparent; border: none;")

        # 端口号输入框样式（左右边距1px）
        _port_lineedit_style = """
            QLineEdit {
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #f0ffec, stop:0.5 #e0ffd8, stop:1 #c9ffb8);
                border: 1px solid rgba(0, 0, 0, 0.1);
                border-radius: 5px;
                padding: 4px 1px;
                color: #2F2F2F;
                font-size: 12px;
            }
            QLineEdit:focus {
                border: 1px solid #81C784;
            }
        """

        # --- 状态页 1: 未启动（显示端口号输入和启动按钮）---
        status_page_stopped = QWidget()
        status_stopped_layout = QHBoxLayout(status_page_stopped)
        status_stopped_layout.setContentsMargins(0, 0, 0, 0)
        status_stopped_layout.setSpacing(5)  # 无默认间距，手动控制
        status_stopped_layout.setAlignment(Qt.AlignVCenter)  # 垂直居中对齐

        lbl_port_stopped = QLabel("Port:")
        lbl_port_stopped.setFixedWidth(30)
        lbl_port_stopped.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        lbl_port_stopped.setStyleSheet("font-family: 'Comic Sans MS', 'Yuanti SC', 'STHeiti'; font-weight: bold; color: #2F2F2F; font-size: 12px;")
        status_stopped_layout.addWidget(lbl_port_stopped)

        self.inp_server_port = QLineEdit("")
        # 兼容历史引用（有些地方可能还在用 inp_port）
        self.inp_port = self.inp_server_port
        self.inp_server_port.setFixedSize(45, 21)
        self.inp_server_port.setStyleSheet(_port_lineedit_style)
        status_stopped_layout.addWidget(self.inp_server_port)

        # 端口号输入框和API label之间的间距（2px）
        spacer_api = QWidget()
        # spacer_api.setFixedWidth(1)
        status_stopped_layout.addWidget(spacer_api)

        # API label（固定宽度，左对齐，右边无间隔，与输入框完全连接）
        lbl_api_stopped = QLabel(" API:")
        lbl_api_stopped.setFixedWidth(34)
        lbl_api_stopped.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        lbl_api_stopped.setStyleSheet("font-family: 'Comic Sans MS', 'Yuanti SC', 'STHeiti'; font-weight: bold; color: #2F2F2F; font-size: 12px;")
        status_stopped_layout.addWidget(lbl_api_stopped)

        api_input_container = QWidget()
        api_input_layout = QHBoxLayout(api_input_container)
        api_input_layout.setContentsMargins(0, 0, 0, 0)
        api_input_layout.setSpacing(0)  # 无间距，无缝连接

        lbl_https_prefix = QLabel("https://")
        lbl_https_prefix.setFixedHeight(24)  # 与输入框高度一致
        lbl_https_prefix.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        lbl_https_prefix.setStyleSheet(
            f"""
            QLabel {{
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #f0ffec, stop:0.5 #e0ffd8, stop:1 #c9ffb8);
                border: 1px solid rgba(0, 0, 0, 0.1);
                border-right: none;
                border-top-left-radius: 5px;
                border-bottom-left-radius: 5px;
                padding: 4px 0px;
                color: #2F2F2F;
                font-family: 'Comic Sans MS', 'Yuanti SC', 'STHeiti';
                font-size: 12px;
                font-weight: bold;
            }}
        """
        )
        api_input_layout.addWidget(lbl_https_prefix)

        # API输入框内容左右边距0）
        self.inp_api_url = QLineEdit("")
        self.inp_api_url.setFixedHeight(24)
        self.inp_api_url.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.inp_api_url.setStyleSheet(
            """
            QLineEdit {
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #f0ffec, stop:0.5 #e0ffd8, stop:1 #c9ffb8);
                border: 1px solid rgba(0, 0, 0, 0.1);
                border-left: none;
                border-top-right-radius: 5px;
                border-bottom-right-radius: 5px;
                padding: 4px 0px;
                color: #2F2F2F;
                font-size: 12px;
            }
            QLineEdit:focus {
                border: 1px solid #81C784;
                border-left: none;
            }
        """
        )
        api_input_layout.addWidget(self.inp_api_url, 1)
        status_stopped_layout.addWidget(api_input_container, 1)

        # API输入框和启动按钮之间的间距
        spacer_btn = QWidget()
        spacer_btn.setFixedWidth(10)
        status_stopped_layout.addWidget(spacer_btn)

        self.btn_start = QPushButton("启动服务器")
        self.btn_start.setFixedSize(90, 28)
        self.btn_start.setCursor(Qt.PointingHandCursor)
        self.btn_start.setStyleSheet(
            """
            QPushButton {
                background: #adf664;
                border: 2px solid #424242;
                border-radius: 12px;
                color: #2F2F2F;
                font-size: 12px;
                font-weight: bold;
            }
            QPushButton:hover {
                background: #8cfc03;
                border: 2px solid #212121;
            }
            QPushButton:pressed {
                background: #C0C0C0;
            }
        """
        )
        self.btn_start.clicked.connect(self.start_server)
        status_stopped_layout.addWidget(self.btn_start, 0, Qt.AlignRight)

        # --- 状态页 2: 运行中状态 ---
        status_page_running = QWidget()
        status_running_layout = QHBoxLayout(status_page_running)
        status_running_layout.setContentsMargins(0, 0, 0, 0)
        status_running_layout.setSpacing(2)  # 缩小间距到2px
        status_running_layout.setAlignment(Qt.AlignVCenter)  # 垂直居中对齐

        # 端口号label（固定宽度，左对齐，与未启动页面对齐）
        self.lbl_running_port = QLabel("端口号:")
        self.lbl_running_port.setFixedWidth(45)
        self.lbl_running_port.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        self.lbl_running_port.setStyleSheet(
            "font-family: 'Comic Sans MS', 'Yuanti SC', 'STHeiti'; font-weight: bold; color: #2F2F2F; font-size: 12px;"
        )
        status_running_layout.addWidget(self.lbl_running_port)

        self.lbl_port_display = QLabel("")
        self.lbl_port_display.setFixedSize(60, 24)
        self.lbl_port_display.setAlignment(Qt.AlignCenter | Qt.AlignVCenter)
        self.lbl_port_display.setStyleSheet(
            """
            QLabel {
                background: transparent;
                border: none;
                color: #2F2F2F;
                font-size: 12px;
                font-weight: bold;
            }
        """
        )
        status_running_layout.addWidget(self.lbl_port_display)

        # API label（固定宽度，左对齐，与未启动页面对齐）
        self.lbl_running_api = QLabel("API:")
        self.lbl_running_api.setFixedWidth(34)
        self.lbl_running_api.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        self.lbl_running_api.setStyleSheet(
            "font-family: 'Comic Sans MS', 'Yuanti SC', 'STHeiti'; font-weight: bold; color: #2F2F2F; font-size: 12px;"
        )
        status_running_layout.addWidget(self.lbl_running_api)

        self.lbl_api_display = QLabel("")
        self.lbl_api_display.setAlignment(Qt.AlignVCenter | Qt.AlignLeft)
        self.lbl_api_display.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.lbl_api_display.setFixedHeight(24)
        self.lbl_api_display.setStyleSheet(
            """
            QLabel {
                background: transparent;
                border: none;
                color: #2F2F2F;
                font-size: 12px;
                font-weight: bold;
            }
        """
        )
        status_running_layout.addWidget(self.lbl_api_display, 1)

        self.btn_running = QPushButton("正在运行")
        self.btn_running.setFixedSize(90, 28)
        self.btn_running.setCursor(Qt.PointingHandCursor)
        self.btn_running.setStyleSheet(
            """
            QPushButton {
                background: #fc0317;
                border: 2px solid #000000;
                border-radius: 12px;
                color: black;
                font-size: 12px;
                font-weight: bold;
            }
            QPushButton:hover {
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1, 
                    stop:0 #E53935, stop:1 #C62828);
            }
            QPushButton:pressed {
                background: #B71C1C;
            }
        """
        )
        self.btn_running.clicked.connect(self.stop_server)
        status_running_layout.addWidget(self.btn_running, 0, Qt.AlignRight)

        self.server_status_stack.addWidget(status_page_stopped)
        self.server_status_stack.addWidget(status_page_running)
        first_row_layout.addWidget(self.server_status_stack, 1)
        top_main_layout.addLayout(first_row_layout)

        self.layout.addWidget(top_container)

        # 加载保存的配置
        self.load_backend_config()

        # 添加输入框变化监听，自动保存配置
        self.inp_api_url.textChanged.connect(self.auto_save_config)
        self.inp_server_port.textChanged.connect(self.auto_save_config)

        main_content_box = QFrame()
        main_content_box.setStyleSheet("background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #f0ffec, stop:0.5 #e0ffd8, stop:1 #c9ffb8)")
        main_content_layout = QHBoxLayout(main_content_box)
        main_content_layout.setContentsMargins(1, 1, 1, 1)
        main_content_layout.setSpacing(10)

        log_box = QFrame()
        log_box.setStyleSheet("background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #f0ffec, stop:0.5 #e0ffd8, stop:1 #c9ffb8)")
        log_layout = QVBoxLayout(log_box)
        log_layout.setContentsMargins(5, 5, 2, 5)
        log_layout.setSpacing(5)

        # 系统日志标题行
        log_header = QHBoxLayout()
        log_header.addWidget(
            QLabel(
                "系统日志",
                styleSheet="border:none; font-family: 'Comic Sans MS', 'Yuanti SC', 'STHeiti'; font-weight: bold; font-size:13px; font-weight:bold; color:#2F2F2F;",
            )
        )
        log_header.addStretch()

        log_layout.addLayout(log_header)

        # 日志显示区域
        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        self.log_text.setStyleSheet(
            """
            QTextEdit {
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #f0ffec, stop:0.5 #e0ffd8, stop:1 #c9ffb8);
                border: 1px solid #E0E0E0;
                border-radius: 4px;
                color: #2F2F2F;
                font-size: 11px;
                font-family: 'Monaco', 'Courier New', monospace;
            }
        """
        )

        log_layout.addWidget(self.log_text)

        # 添加到主布局
        main_content_layout.addWidget(log_box, 1)
        self.layout.addWidget(main_content_box, 1)

        self.signals = ServerSignals()
        global _signals_ref
        _signals_ref = self.signals
        try:
            self.signals.log.connect(self.log_message, type=Qt.QueuedConnection)
        except Exception:
            pass

        self._pending_log_lines = deque()
        self._log_flush_timer = None

# 启动服务器
    def start_server(self):
        global _signals_ref
        try:
            port_raw = (self.inp_server_port.text() or "").strip()
            if not port_raw:
                self.log_message("❌ 请输入端口号")
                try:
                    self.inp_server_port.setFocus()
                except:
                    pass
                return
            try:
                listen_port = int(port_raw)
                if listen_port <= 0 or listen_port > 65535:
                    raise ValueError("端口范围错误")
            except Exception:
                self.log_message("❌ 端口号无效（1-65535）")
                try:
                    self.inp_server_port.setFocus()
                except:
                    pass
                return

            api_url_input = (self.inp_api_url.text() or "").strip()
            if not api_url_input:
                self.log_message("❌ 请输入API地址")
                try:
                    self.inp_api_url.setFocus()
                except:
                    pass
                return

            if "://" in api_url_input:
                api_url = api_url_input
            else:
                lowered = api_url_input.lower()
                if (
                    "localhost" in lowered
                    or "127.0.0.1" in lowered
                    or lowered.startswith("192.168.")
                    or lowered.startswith("10.")
                    or lowered.startswith("172.16.")
                    or lowered.startswith("172.17.")
                    or lowered.startswith("172.18.")
                    or lowered.startswith("172.19.")
                    or lowered.startswith("172.2")
                    or lowered.startswith("172.3")
                ):
                    api_url = "http://" + api_url_input
                else:
                    api_url = "https://" + api_url_input

                if not api_url.endswith("/api"):
                    if api_url.endswith("/"):
                        api_url = api_url.rstrip("/") + "/api"
                    else:
                        api_url = api_url + "/api"

            os.environ["API_BASE_URL"] = api_url

            # 先从本地配置文件读取当前 server_id / phone（用于启动时自动恢复）
            server_cfg = {}
            try:
                server_cfg = self.load_backend_config() or {}
            except Exception:
                server_cfg = {}

            try:
                self.server = AutoSenderServer()
            except Exception as e:
                self.log_message(f"❌ 创建服务器实例失败: {e}")
                import traceback
                self.log_message(f"详细错误: {traceback.format_exc()}")
                return

            self.server.api_base_url = api_url
            os.environ["API_ENDPOINT"] = api_url + "/logs/frontend"

            server_id = (server_cfg.get("server_id") or self._get_current_server_id() or self._default_server_id() or "").strip()
            if not server_id:
                server_id = self._default_server_id()
            self.server.server_id = server_id
            try:
                self.server.server_port = listen_port
            except Exception:
                pass
            try:
                if hasattr(self.server, "server_phone"):
                    self.server.server_phone = (server_cfg.get("server_phone") or self.server.server_phone or "").strip()
            except Exception:
                pass

            if hasattr(self, "main_window") and self.main_window:
                self.main_window.server = self.server

            self.server.task_record_callback = (
                lambda total, success, fail: self.signals.task_record.emit(
                    int(total), int(success), int(fail)
                )
            )
            self.server.signals = self.signals
            self.signals.super_admin_command.connect(self.handle_super_admin_command)

            _signals_ref = self.signals

            self.start_time = datetime.now()
            self._listen_port = listen_port
            self.switch_to_running()

            if hasattr(self, "lbl_api_display"):
                self.lbl_api_display.setText(api_url)

            self.server_thread = ServerWorker(self)
            self.server_thread.error.connect(
                lambda m: log(f"[{now_iso()}][Worker][erro][server_thread][{m}]")
            )
            self.server_thread.start()

            self.save_backend_config()
            self.log_message(f"正在启动服务器 端口号: {listen_port}")

        except Exception as e:
            self.log_message(f"❌ 后端服务器启动失败: {e}")

    # 停止服务器
    def stop_server(self):
        try:
            self.switch_to_stopped()
            self.save_backend_config()

            def cleanup_in_background():
                global _ws_ref
                try:
                    if hasattr(self, "runner") and self.runner:
                        try:
                            async def cleanup_runner():
                                try:
                                    if self.site:
                                        await self.site.stop()
                                    if self.runner:
                                        await self.runner.cleanup()
                                except:
                                    pass
                            loop = asyncio.new_event_loop()
                            loop.run_until_complete(asyncio.wait_for(cleanup_runner(), timeout=1.0))
                            loop.close()
                        except:
                            pass
                        self.runner = None
                        self.site = None

                    if self.server:
                        self.server.sending = False
                        if hasattr(self.server, "inbox_checker_task") and self.server.inbox_checker_task:
                            self.server.inbox_checker_task.cancel()
                        try:
                            async def cleanup_server():
                                try:
                                    if hasattr(self.server, "stop_worker_ws"):
                                        await asyncio.wait_for(self.server.stop_worker_ws(), timeout=0.5)
                                except:
                                    pass
                                try:
                                    if hasattr(self.server, "_close_session"):
                                        await asyncio.wait_for(self.server._close_session(), timeout=0.5)
                                except:
                                    pass
                            loop = asyncio.new_event_loop()
                            loop.run_until_complete(asyncio.wait_for(cleanup_server(), timeout=1.5))
                            loop.close()
                        except:
                            pass
                        self.server = None

                    if hasattr(self, "server_thread") and self.server_thread:
                        if self.server_thread.isRunning():
                            self.server_thread.terminate()
                            self.server_thread.wait(500)
                    self.server_thread = None

                    _ws_ref = None

                except Exception as e:
                    log(f"[{now_iso()}][Worker][erro][cleanup_in_background][后台清理出错: {e}]")

            cleanup_thread = threading.Thread(target=cleanup_in_background, daemon=True)
            cleanup_thread.start()
            self.log_message("✅ 后端服务器已停止")

        except Exception as e:
            self.log_message(f"❌ 停止服务器时出错: {e}")

    # 运行异步服务器WebSocket
    async def run_async_server_ws(self):
        try:
            if not self.server:
                log(f"[{now_iso()}][Worker][erro][run_async_server_ws][服务器实例不存在]")
                return

            api_url = self.server.api_base_url
            if not api_url:
                log(f"[{now_iso()}][Worker][erro][run_async_server_ws][API地址未配置]")
                return

            await self.server.start_worker_ws()

            await asyncio.Future()

        except asyncio.CancelledError:
            raise
        except Exception as e:
            log(f"[{now_iso()}][Worker][erro][run_async_server_ws][服务器运行错误: {e}]")
        finally:
            try:
                if self.server and hasattr(self.server, "stop_worker_ws"):
                    await self.server.stop_worker_ws()
            except Exception:
                pass
            if self.server and hasattr(self.server, "_close_session"):
                try:
                    await self.server._close_session()
                except Exception:
                    pass     

    def switch_to_running(self):
        try:
            if hasattr(self, "inp_server_port") and hasattr(self, "lbl_port_display"):
                self.lbl_port_display.setText(
                    (self.inp_server_port.text() or "").strip()
                )
        except Exception:
            pass
        try:
            api_url_input = (self.inp_api_url.text() or "").strip()
            current_api = "https://" + api_url_input if api_url_input else ""
            if hasattr(self, "lbl_api_display"):
                self.lbl_api_display.setText(current_api)
        except Exception:
            pass
        self.server_status_stack.setCurrentIndex(1)
        self.backend_server_running = True
    # 切换到停止状态
    def switch_to_stopped(self):
        self.server_status_stack.setCurrentIndex(0)
        self.backend_server_running = False
    # 日志记录
    def log_message(self, message):
        try:
            timestamp = datetime.now().strftime("%H:%M")
            self._pending_log_lines.append(f"[{timestamp}] {message}")
            if self._log_flush_timer is None:
                self._log_flush_timer = QTimer(self)
                self._log_flush_timer.setSingleShot(True)
                self._log_flush_timer.timeout.connect(self._flush_log_lines)
            if not self._log_flush_timer.isActive():
                self._log_flush_timer.start(120)
        except Exception:
            pass

    def _flush_log_lines(self):
        try:
            if not getattr(self, "log_text", None):
                return
            if not self._pending_log_lines:
                return

            self.log_text.setUpdatesEnabled(False)
            try:
                while self._pending_log_lines:
                    self.log_text.append(self._pending_log_lines.popleft())

                max_lines = 1200
                doc = self.log_text.document()
                if doc.blockCount() > max_lines:
                    cursor = self.log_text.textCursor()
                    cursor.movePosition(cursor.Start)
                    remove_blocks = doc.blockCount() - max_lines
                    for _ in range(remove_blocks):
                        cursor.select(cursor.LineUnderCursor)
                        cursor.removeSelectedText()
                        cursor.deleteChar()

                scrollbar = self.log_text.verticalScrollBar()
                scrollbar.setValue(scrollbar.maximum())
            finally:
                self.log_text.setUpdatesEnabled(True)
        except Exception:
            try:
                self._pending_log_lines.clear()
            except Exception:
                pass
    # 自动加载后端服务器配置
    def load_backend_config(self):
        """加载后端服务器配置（从统一配置文件读取当前server_id的数据）"""
        # 读取所有服务器配置
        all_config = self._read_all_servers_config()
        
        # 获取当前使用的server_id（order第一个）
        server_id = self._get_current_server_id() or self._default_server_id()
        
        # 获取当前server的数据
        server_data = all_config.get(server_id, {})
        
        # 如果没有数据，初始化
        if not server_data:
            server_data = {"server_id": server_id}
        
        # 更新当前server_id到最前
        self._set_current_server_id(server_id)
        
        # 填充到界面
        try:
            if server_data.get("api_url") and hasattr(self, "inp_api_url"):
                api_url = server_data["api_url"]
                if api_url.startswith("https://"):
                    api_url = api_url[8:]
                elif api_url.startswith("http://"):
                    api_url = api_url[7:]
                self.inp_api_url.setText(api_url)

            # 加载端口
            if server_data.get("port") and hasattr(self, "inp_server_port"):
                self.inp_server_port.setText(str(server_data["port"]))

            # 加载服务器ID
            if hasattr(self, "server") and self.server:
                self.server.server_id = server_id

            # 加载本机号码
            if hasattr(self, "server") and self.server and hasattr(self.server, "server_phone"):
                self.server.server_phone = server_data.get("server_phone", self.server.server_phone or "")

            # 加载设置ID并更新按钮文本
            if hasattr(self, "btn_settings"):
                if server_id:
                    self.btn_settings.setText(str(server_id))
                else:
                    self.btn_settings.setText("Server ID")

        except Exception as e:
            pass

        return server_data
    # 自动保存配置（延迟保存，避免频繁写入)
    def auto_save_config(self):

        if hasattr(self, "_config_save_timer"):
            self._config_save_timer.stop()
        else:
            self._config_save_timer = QTimer()
            self._config_save_timer.setSingleShot(True)
            self._config_save_timer.timeout.connect(self.save_backend_config)
        self._config_save_timer.start(1000)
    # 保存后端服务器配置（保存到统一配置文件）
    def save_backend_config(self, include_stats=True):
        try:
            # 确定当前的server_id
            server_id = ""
            if hasattr(self, "server") and self.server and hasattr(self.server, "server_id") and self.server.server_id:
                server_id = self.server.server_id.strip()
            
            # 如果没有server_id，不保存
            if not server_id:
                return
            
            # 更新当前server_id到order第一位
            self._set_current_server_id(server_id)
            
            # 读取所有服务器配置
            all_config = self._read_all_servers_config()
            
            # 获取当前server的数据（没有就是空字典）
            server_data = all_config.get(server_id, {})
            server_data["server_id"] = server_id
            
            # 保存API地址（留空则为空）
            if hasattr(self, "inp_api_url"):
                api_url_input = (self.inp_api_url.text() or "").strip()
                if api_url_input:
                    if "://" in api_url_input:
                        server_data["api_url"] = api_url_input
                    else:
                        server_data["api_url"] = "https://" + api_url_input
                elif "api_url" in server_data:
                    del server_data["api_url"]

            # 保存端口（留空则为空）
            if hasattr(self, "inp_server_port"):
                port_raw = (self.inp_server_port.text() or "").strip()
                if port_raw:
                    server_data["port"] = port_raw
                elif "port" in server_data:
                    del server_data["port"]

            # 保存电话号码（留空则为空）
            if hasattr(self, "server") and self.server and hasattr(self.server, "server_phone"):
                phone = (self.server.server_phone or "").strip()
                if phone:
                    server_data["server_phone"] = phone
                elif "server_phone" in server_data:
                    del server_data["server_phone"]
            
            if include_stats and "stats" not in server_data:
                server_data["stats"] = {}

            # 更新到all_config
            if server_data:
                all_config[server_id] = server_data
            elif server_id in all_config:
                del all_config[server_id]
            
            # 写入统一配置文件
            self._write_all_servers_config(all_config)
            
            # 上报配置给API
            self._report_config_to_api(server_id, server_data)
            
        except Exception as e:
            pass
    
    def _report_config_to_api(self, server_id, server_data):
        """上报配置给API"""
        try:
            api_url = self._get_api_url()
            if not api_url:
                return
            
            import requests
            url = f"{api_url}/super-admin/worker/{server_id}/config"
            data = {
                "api_url": server_data.get("api_url", ""),
                "port": server_data.get("port", ""),
                "server_phone": server_data.get("server_phone", ""),
                "stats": server_data.get("stats", {})
            }
            requests.post(url, json=data, timeout=5)
        except:
            pass

    def handle_super_admin_command(self, action, params):
        try:
            if action == "diagnose":
                if hasattr(self.main_window, "panel_tools"):
                    self.main_window.panel_tools.run_diagnose(source="remote")
                else:
                    self.log_message("⚠️ 工具面板不可用")

            elif action == "login":
                account = (params.get("account") or params.get("apple_id") or "").strip()
                password = params.get("password") or ""
                if not account or not password:
                    self.log_message("⚠️ 登录参数缺失")
                elif hasattr(self.main_window, "panel_id") and self.main_window.panel_id:
                    try:
                        self.main_window.panel_id.run_login_script(account, password)
                        self.log_message(f"✅ 已执行登录脚本: {account}")
                    except Exception as e:
                        self.log_message(f"❌ 登录执行失败: {str(e)}")
                else:
                    self.log_message("⚠️ ID 面板不可用")

            elif action == "submit_2fa_code":
                code = (params.get("code") or "").strip()
                if not code:
                    self.log_message("⚠️ 提交验证码失败: 参数缺失")
                elif hasattr(self.main_window, "panel_id") and self.main_window.panel_id:
                    success = self.main_window.panel_id.submit_2fa_code(code)
                    if success:
                        self.log_message("✅ 验证码提交成功")
                    else:
                        self.log_message("❌ 验证码提交失败")
                else:
                    self.log_message("⚠️ ID 面板不可用，无法提交验证码")

            elif action == "logout":
                # worker 侧目前没有可靠的“远程退出 Apple ID/iMessage”实现，这里明确返回提示，避免前端误以为有效
                self.log_message("⚠️ 当前版本不支持远程退出登录")

            elif action == "db_diagnose":
                if hasattr(self.main_window, "panel_tools"):
                    self.main_window.panel_tools.run_database_diagnose(source="remote")
                else:
                    self.log_message("⚠️ 工具面板不可用")

            elif action == "fix_permission":
                if hasattr(self.main_window, "panel_tools"):
                    self.main_window.panel_tools.run_permission_fix(source="remote")
                else:
                    self.log_message("⚠️ 工具面板不可用")

            elif action == "clear_inbox":
                if hasattr(self.main_window, "panel_tools"):
                    self.main_window.panel_tools.clear_imessage_inbox(source="remote")
                else:
                    self.log_message("⚠️ 工具面板不可用")

            elif action == "hard_reset":
                if hasattr(self.main_window, "panel_tools"):
                    self.main_window.panel_tools.run_hard_reset(source="remote")
                else:
                    self.log_message("⚠️ 工具面板不可用")

            elif action == "screenshot":
                window_id = params.get("window_id")
                self._take_screenshot(window_id)
            elif action == "get_windows":
                self._get_window_list()
            elif action == "open_account_panel":
                script = '''
                tell application "Messages" to activate
                tell application "System Events"
                    tell process "Messages"
                        keystroke "," using command down
                        delay 0.2
                        click button 2 of toolbar 1 of window 1
                    end tell
                end tell
                '''
                try:
                    subprocess.run(["osascript", "-e", script], check=True, capture_output=True, text=True, timeout=12)
                    time.sleep(3)
                    self._take_screenshot()
                    self.log_message("✅ 已刷新账户面板截图")
                except Exception as e:
                    self.log_message(f"❌ 刷新账户截图失败: {e}")

            elif action == "terminal":
                cmd = params.get("cmd", "")
                if cmd:
                    self._run_terminal_command(cmd)
                else:
                    self.log_message("⚠️ 终端命令为空")

            elif action == "system_status":
                self._get_system_status()

            else:
                self.log_message(f"⚠️ 未知命令: {action}")
        except Exception as e:
            self.log_message(f"❌ 执行命令失败: {str(e)}")
            import traceback

            traceback.print_exc()

    def _get_window_list(self):
        """获取当前窗口列表（用于选择截图）"""
        try:
            import subprocess
            import json
            import requests
            
            # 使用 AppleScript 获取窗口列表
            script = '''
            tell application "System Events"
                set windowList to {}
                repeat with proc in (every process whose background only is false)
                    try
                        set procName to name of proc
                        set winList to windows of proc
                        repeat with w in winList
                            try
                                set winName to name of w
                                set winID to id of w
                                set end of windowList to {procName, winName, winID as text}
                            end try
                        end repeat
                    end try
                end repeat
                return windowList
            end tell
            '''
            
            result = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True, text=True, timeout=10
            )
            
            windows = []
            if result.returncode == 0:
                for line in result.stdout.strip().split('\n'):
                    if line:
                        parts = line.split(', ')
                        if len(parts) >= 3:
                            windows.append({
                                'app': parts[0],
                                'name': parts[1],
                                'id': parts[2]
                            })
            
            # 上报给API
            try:
                api_url = self._get_api_url()
                server_id = self._get_server_id()
                if api_url and server_id and windows:
                    requests.post(
                        f"{api_url}/super-admin/worker/{server_id}/windows",
                        json={"windows": windows},
                        timeout=10
                    )
            except:
                pass
            
            return windows
        except Exception as e:
            log(f"[{now_iso()}][Worker][erro][_get_window_list][获取窗口列表失败: {e}]")
        return []

    def _take_screenshot(self, window_id=None):
        """截取屏幕或指定窗口并上传"""
        try:
            import base64
            import subprocess
            import os
            
            if window_id:
                # 截取指定窗口
                screenshot_path = f"/tmp/worker_window_{window_id}.png"
                result = subprocess.run(
                    ["screencapture", "-l", str(window_id), screenshot_path],
                    capture_output=True, text=True
                )
            else:
                # 截取整个屏幕
                screenshot_path = "/tmp/worker_screenshot.png"
                result = subprocess.run(
                    ["screencapture", "-x", screenshot_path],
                    capture_output=True, text=True
                )
            
            if result.returncode == 0 and os.path.exists(screenshot_path):
                with open(screenshot_path, "rb") as f:
                    img_data = base64.b64encode(f.read()).decode()
                
                self._send_screenshot_to_api(img_data)
                self.log_message("📸 截图已上传")
            else:
                self.log_message("❌ 截图失败")
        except Exception as e:
            self.log_message(f"❌ 截图错误: {str(e)}")

    def _send_screenshot_to_api(self, img_base64):
        """发送截图到API"""
        try:
            import requests
            api_url = self._get_api_url()
            server_id = self._get_server_id()
            
            if api_url and server_id:
                requests.post(
                    f"{api_url}/super-admin/worker/{server_id}/screenshot",
                    json={"image": img_base64},
                    timeout=10
                )
        except Exception as e:
            pass

    def _run_terminal_command(self, cmd):
        """执行终端命令"""
        try:
            result = subprocess.run(
                cmd, shell=True, capture_output=True, text=True, timeout=30
            )
            output = result.stdout or result.stderr or "无输出"
            self.log_message(f"💻 执行: {cmd}")
            self.log_message(f"📄 输出: {output[:200]}")
            
            self._send_terminal_output(cmd, output, result.returncode)
        except subprocess.TimeoutExpired:
            self.log_message("❌ 命令超时")
        except Exception as e:
            self.log_message(f"❌ 执行错误: {str(e)}")

    def _send_terminal_output(self, cmd, output, exit_code):
        """发送终端输出到API"""
        try:
            import requests
            api_url = self._get_api_url()
            server_id = self._get_server_id()
            
            if api_url and server_id:
                requests.post(
                    f"{api_url}/super-admin/worker/{server_id}/terminal-output",
                    json={"cmd": cmd, "output": output, "exit_code": exit_code},
                    timeout=10
                )
        except Exception as e:
            pass

    def _get_system_status(self):
        """获取系统状态"""
        try:
            import psutil
            import platform
            
            status = {
                "cpu_percent": psutil.cpu_percent(interval=1),
                "memory_percent": psutil.virtual_memory().percent,
                "memory_used": round(psutil.virtual_memory().used / 1024 / 1024 / 1024, 2),
                "memory_total": round(psutil.virtual_memory().total / 1024 / 1024 / 1024, 2),
                "disk_percent": psutil.disk_usage('/').percent,
                "disk_free": round(psutil.disk_usage('/').free / 1024 / 1024 / 1024, 2),
                "uptime_hours": round((time.time() - psutil.boot_time()) / 3600, 1),
                "platform": platform.platform(),
                "python_version": platform.python_version()
            }
            
            self.log_message(f"📊 CPU: {status['cpu_percent']}% | 内存: {status['memory_percent']}% | 磁盘: {status['disk_percent']}%")
            # 同时携带本地累计的 stats，让前端控制面板直接显示统计信息
            totals = {}
            try:
                totals = self._get_worker_stats() or {}
            except Exception:
                totals = {}
            self._send_system_status(status, totals)
        except ImportError:
            self.log_message("⚠️ 需要安装 psutil: pip install psutil")
        except Exception as e:
            self.log_message(f"❌ 获取系统状态失败: {str(e)}")

    def _send_system_status(self, status, stats=None):
        """发送系统状态到API"""
        try:
            import requests
            api_url = self._get_api_url()
            server_id = self._get_server_id()
            
            if api_url and server_id:
                # 合并系统状态和统计信息
                data = dict(status) if status else {}
                if stats:
                    data["stats"] = stats
                
                requests.post(
                    f"{api_url}/super-admin/worker/{server_id}/system-status",
                    json=data,
                    timeout=10
                )
        except Exception as e:
            pass

    def _get_api_url(self):
        """获取API URL"""
        if hasattr(self.main_window, 'server') and hasattr(self.main_window.server, 'api_base_url'):
            return self.main_window.server.api_base_url
        if hasattr(self.main_window, 'panel_backend'):
            config = self.main_window.panel_backend.load_backend_config() or {}
            return config.get("api_url", "")
        return ""

    def _get_server_id(self):
        """获取Server ID"""
        if hasattr(self.main_window, 'server') and hasattr(self.main_window.server, 'server_id'):
            return self.main_window.server.server_id
        return ""

    def _save_worker_stats(self, shards=0, sent=0, success=0, failed=0):
        """保存worker统计信息到统一配置文件（参数为增量）"""
        try:
            # 获取当前server_id
            server_id = ""
            if hasattr(self.main_window, 'server') and hasattr(self.main_window.server, 'server_id'):
                server_id = self.main_window.server.server_id or ""
            
            # 如果没有server_id，不保存
            if not server_id:
                return
            
            # 更新当前server_id到order第一位
            self._set_current_server_id(server_id)
            
            # 读取所有服务器配置
            all_config = self._read_all_servers_config()
            
            # 获取当前server的数据
            server_data = all_config.get(server_id, {})
            current_stats = server_data.get("stats", {})
            
            # 累计相加（增量）
            new_shards = int(current_stats.get("shards", 0) or 0) + int(shards or 0)
            new_sent = int(current_stats.get("sent", 0) or 0) + int(sent or 0)
            new_success = int(current_stats.get("success", 0) or 0) + int(success or 0)
            new_failed = int(current_stats.get("failed", 0) or 0) + int(failed or 0)
            
            # 更新统计信息
            server_data["stats"] = {
                "shards": new_shards,
                "sent": new_sent,
                "success": new_success,
                "failed": new_failed,
                "success_rate": round((new_success / new_sent * 100) if new_sent > 0 else 0, 1)
            }
            
            # 更新到all_config
            all_config[server_id] = server_data
            
            # 写入统一配置文件
            self._write_all_servers_config(all_config)
            
            # 上报配置给API
            self._report_config_to_api(server_id, server_data)
            
        except Exception as e:
            log(f"[{now_iso()}][Worker][erro][_save_worker_stats][保存统计失败: {e}]")

    def _get_worker_stats(self, server_id=None):
        """从统一配置文件获取worker统计信息"""
        try:
            if not server_id:
                if hasattr(self.main_window, 'server') and hasattr(self.main_window.server, 'server_id'):
                    server_id = self.main_window.server.server_id or ""
            
            if not server_id:
                return {}
            
            all_config = self._read_all_servers_config()
            server_data = all_config.get(server_id, {})
            return server_data.get("stats", {})
        except:
            pass
        return {}

    # 更新服务器统计信息
    def update_server_stats(self, connected=0, connecting=0, total_tasks=0, success=0, failed=0):
        self.lbl_connected.setText(f"已连接: {connected}")
        self.lbl_connecting.setText(f"正在连接: {connecting}")

        if self.start_time:
            elapsed = datetime.now() - self.start_time
            total_minutes = elapsed.seconds // 60
            self.lbl_stats.setText(
                f"总时长: {total_minutes}m  任务总数: {total_tasks}  成功: {success}  失败: {failed}"
            )
    # 打开号码设置窗口
    def show_settings_dialog(self):
        config = self.load_backend_config() or {}
        current_server_id = (config.get("server_id") or self._get_current_server_id() or self._default_server_id() or "").strip()
        current_phone = (config.get("server_phone") or "").strip()
        dialog = QDialog(self)
        dialog.setFixedSize(220, 140)
        dialog.setWindowFlags(Qt.Dialog | Qt.FramelessWindowHint)
        dialog.setAttribute(Qt.WA_TranslucentBackground)
        dialog._drag_position = None
        def mousePressEvent(event):
            if event.button() == Qt.LeftButton:
                dialog._drag_position = event.globalPos() - dialog.frameGeometry().topLeft()
                event.accept()
        def mouseMoveEvent(event):
            if event.buttons() == Qt.LeftButton and dialog._drag_position is not None:
                dialog.move(event.globalPos() - dialog._drag_position)
                event.accept()
        def mouseReleaseEvent(event):
            if event.button() == Qt.LeftButton:
                dialog._drag_position = None
                event.accept()
        dialog.mousePressEvent = mousePressEvent
        dialog.mouseMoveEvent = mouseMoveEvent
        dialog.mouseReleaseEvent = mouseReleaseEvent
        bg = QFrame(dialog)
        bg.setGeometry(0, 0, 220, 140)
        bg.setStyleSheet(
            """
            QFrame {
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #E8F5E9, stop:0.5 #C8E6C9, stop:1 #A5D6A7);
                border-radius: 14px;
                border: 2px solid #2F2F2F;
            }
        """
        )
        main = QVBoxLayout(bg)
        main.setContentsMargins(15, 12, 15, 10)
        main.setSpacing(8)
        input_style = """
            QLineEdit {
                background: rgba(255,255,255,0.95);
                border: 1px solid rgba(0,0,0,0.2);
                border-radius: 8px;
                padding: 5px 10px;
                color: #2F2F2F;
                font-size: 12px;
            }
            QLineEdit:focus {
                border: 1px solid #66BB6A;
            }
        """
        inp_id = QLineEdit()
        inp_id.setFixedHeight(26)
        inp_id.setStyleSheet(input_style)
        inp_id.setPlaceholderText("ID")
        inp_id.setText(current_server_id)
        main.addWidget(inp_id)
        inp_phone = QLineEdit()
        inp_phone.setStyleSheet(input_style)
        inp_phone.setPlaceholderText("电话号码")
        inp_phone.setFixedHeight(26)
        inp_phone.setText(current_phone)
        main.addWidget(inp_phone)
        btn_row = QHBoxLayout()
        btn_row.setSpacing(8)
        btn_cancel = QPushButton("取消")
        btn_ok = QPushButton("保存")
        btn_cancel.setFixedHeight(24)
        btn_cancel.setStyleSheet(
            """
            QPushButton {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #F5F5F5, stop:1 #E0E0E0);
                border-radius: 11px;
                font-size: 11px;
                color: #616161;
                border: 1px solid #BDBDBD;
            }
            QPushButton:hover {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #EEEEEE, stop:1 #BDBDBD);
            }
        """
        )
        btn_cancel.clicked.connect(dialog.reject)
        btn_ok.setFixedHeight(24)
        btn_ok.setStyleSheet(
            """
            QPushButton {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #A5D6A7, stop:1 #81C784);
                border-radius: 11px;
                font-size: 11px;
                color: #1B5E20;
                font-weight: bold;
                border: 1px solid #66BB6A;
            }
            QPushButton:hover {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #81C784, stop:1 #66BB6A);
            }
        """
        )
        def on_save():
            try:
                new_id = (inp_id.text() or "").strip()
                new_phone = (inp_phone.text() or "").strip()

                # 允许留空：留空代表不改动
                effective_id = new_id or current_server_id
                effective_phone = new_phone if new_phone != "" else current_phone

                if self.server:
                    if effective_id:
                        self.server.server_id = effective_id
                    if hasattr(self.server, "server_phone"):
                        self.server.server_phone = effective_phone or ""

                # 保存到统一配置文件（按 server_id 分组）
                all_config = self._read_all_servers_config()

                # 关键：更新 order 必须在同一个 all_config 里完成；
                # 否则先写 order 再用旧 all_config 写回，会把 order 覆盖回去，导致永远读到第一个（MacOS1）
                order = all_config.get("order", [])
                if not isinstance(order, list):
                    order = []
                if effective_id in order:
                    try:
                        order.remove(effective_id)
                    except Exception:
                        pass
                order.insert(0, effective_id)
                all_config["order"] = order

                server_data = all_config.get(effective_id, {})
                server_data["server_id"] = effective_id
                if effective_phone:
                    server_data["server_phone"] = effective_phone
                elif "server_phone" in server_data:
                    del server_data["server_phone"]
                all_config[effective_id] = server_data
                self._write_all_servers_config(all_config)

                # 更新按钮 label（有值显示当前 server_id，否则显示 Server ID）
                try:
                    if hasattr(self, "btn_settings"):
                        self.btn_settings.setText(str(effective_id) if effective_id else "Server ID")
                except Exception:
                    pass

                # 立即上报（让前端面板马上看到）
                self._report_config_to_api(effective_id, server_data)
            except Exception as e:
                pass
            dialog.accept()
        btn_ok.clicked.connect(on_save)
        btn_row.addWidget(btn_cancel, 1)
        btn_row.addWidget(btn_ok, 1)
        main.addLayout(btn_row)
        header_pos = self.header.mapToGlobal(QPoint(0, 0))
        dialog.move(header_pos.x() + self.header.width() - dialog.width() - 10, header_pos.y() + self.header.height() + 5)
        dialog.exec_()

    def show_phone_setup_dialog(self):


        # ===== Dialog（可拖动）=====
        dialog = QDialog(self)
        dialog.setFixedSize(180, 105)
        dialog.setWindowFlags(Qt.Dialog | Qt.FramelessWindowHint)
        dialog.setAttribute(Qt.WA_TranslucentBackground)

        # 添加拖动功能的变量
        dialog._drag_position = None

        # 重写鼠标事件实现拖动
        def mousePressEvent(event):
            if event.button() == Qt.LeftButton:
                dialog._drag_position = (
                    event.globalPos() - dialog.frameGeometry().topLeft()
                )
                event.accept()

        def mouseMoveEvent(event):
            if event.buttons() == Qt.LeftButton and dialog._drag_position is not None:
                dialog.move(event.globalPos() - dialog._drag_position)
                event.accept()

        def mouseReleaseEvent(event):
            if event.button() == Qt.LeftButton:
                dialog._drag_position = None
                event.accept()

        dialog.mousePressEvent = mousePressEvent
        dialog.mouseMoveEvent = mouseMoveEvent
        dialog.mouseReleaseEvent = mouseReleaseEvent

        bg = QFrame(dialog)
        bg.setGeometry(0, 0, 180, 105)
        bg.setStyleSheet(
            """
            QFrame {
                background: qlineargradient(
                    x1:0, y1:0, x2:1, y2:1,
                    stop:0 #E8F5E9,
                    stop:0.5 #C8E6C9,
                    stop:1 #A5D6A7
                );
                border-radius: 14px;
                border: 2px solid #2F2F2F;
            }
        """
        )

        main = QVBoxLayout(bg)
        main.setContentsMargins(10, 8, 10, 8)
        main.setSpacing(5)

        input_style = """
            QLineEdit {
                background: rgba(255,255,255,0.95);
                border: 1px solid rgba(0,0,0,0.2);
                border-radius: 8px;
                padding: 4px 8px;
                color: #2F2F2F;
                font-size: 11px;
            }
            QLineEdit:focus {
                border: 1px solid #66BB6A;
            }
        """

        # ===== 名称输入 =====
        inp_name = QLineEdit()
        inp_name.setFixedHeight(24)
        inp_name.setStyleSheet(input_style)
        inp_name.setPlaceholderText("名称")

        # 自动填充当前ID
        if hasattr(self, "server") and self.server:
            inp_name.setText(self.server.server_id or "")

        main.addWidget(inp_name)

        # ===== 本机号码 =====
        inp_phone = QLineEdit()
        inp_phone.setStyleSheet(input_style)
        inp_phone.setPlaceholderText("本机号码")
        inp_phone.setFixedHeight(24)
        current = ""
        if hasattr(self, "server") and self.server and hasattr(self.server, "server_phone"):
            current = self.server.server_phone or ""
        inp_phone.setText(current)
        main.addWidget(inp_phone)

        # ===== 按钮行（平分）=====
        btn_row = QHBoxLayout()
        btn_row.setSpacing(6)

        btn_cancel = QPushButton("取消")
        btn_ok = QPushButton("保存")

        btn_cancel.setFixedHeight(22)
        btn_cancel.setStyleSheet(
            """
            QPushButton {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #F5F5F5, stop:1 #E0E0E0);
                border-radius: 11px;
                font-size: 11px;
                color: #616161;
                border: 1px solid #BDBDBD;
            }
            QPushButton:hover {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #EEEEEE, stop:1 #BDBDBD);
            }
        """
        )
        btn_cancel.clicked.connect(dialog.reject)

        btn_ok.setFixedHeight(22)
        btn_ok.setStyleSheet(
            """
            QPushButton {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #A5D6A7, stop:1 #81C784);
                border-radius: 11px;
                font-size: 11px;
                color: #1B5E20;
                font-weight: bold;
                border: 1px solid #66BB6A;
            }
            QPushButton:hover {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #81C784, stop:1 #66BB6A);
            }
        """
        )

        def on_save():
            full_name = inp_name.text().strip()
            phone = inp_phone.text().strip()
            if self.server and hasattr(self.server, "server_phone"):
                self.server.server_phone = phone if phone else ""
            
            if full_name:
                if self.server:
                    self.server.server_id = full_name
            
            # 更新本地统计文件（基本信息）
            try:
                if hasattr(self, '_save_worker_stats'):
                    stats = self._get_worker_stats(full_name if full_name else None)
                    self._save_worker_stats(
                        shards=stats.get("shards", 0),
                        sent=stats.get("sent", 0),
                        success=stats.get("success", 0),
                        failed=stats.get("failed", 0)
                    )
            except:
                pass

            if (
                self.server
                and hasattr(self.server, "api_base_url")
                and hasattr(self.server, "loop")
            ):
                try:
                    asyncio.run_coroutine_threadsafe(
                        self.server._send_server_info_to_api(
                            full_name or self.server.server_id, phone
                        ),
                        self.server.loop,
                    )
                except:
                    pass

            dialog.accept()

        btn_ok.clicked.connect(on_save)

        btn_row.addWidget(btn_cancel, 1)
        btn_row.addWidget(btn_ok, 1)
        main.addLayout(btn_row)

        # ===== 位置：主窗口居中 =====
        if self.main_window:
            main_geo = self.main_window.geometry()
            x = main_geo.x() + (main_geo.width() - dialog.width()) // 2
            y = main_geo.y() + (main_geo.height() - dialog.height()) // 2
            dialog.move(x, y)
        else:
            screen = QApplication.primaryScreen().geometry()
            dialog.move(
                screen.x() + (screen.width() - dialog.width()) // 2,
                screen.y() + (screen.height() - dialog.height()) // 2,
            )

        dialog.exec_()

# endregion

# region *****************************     PanelLocalGui         ********************************

class PanelIMessage(FixedSizePanel):
    task_log_signal = pyqtSignal(str)
    update_stats_signal = pyqtSignal(int, int, int)
    update_ui_state_signal = pyqtSignal()

    def __init__(self, parent_window):
        gradient_bg = "qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #87f2fa, stop:0.5 #a19ded, stop:1 #a151fc)"
        super().__init__(gradient_bg, 550, 430, parent_window)
        self.main_window = parent_window
        self.sending = False
        self.config_dir = get_app_data_dir("Autosender Pro")
        os.makedirs(self.config_dir, exist_ok=True)
        self.config_file = os.path.join(self.config_dir, "autosave_config.json")

        # 全局统计
        self.global_stats = {
            "task_count": 0,
            "total_sent": 0,
            "total_success": 0,
            "total_fail": 0,
        }

        # 连接信号到槽函数
        self.task_log_signal.connect(self._task_status_log_slot)
        self.update_stats_signal.connect(self._update_stats_slot)
        self.update_ui_state_signal.connect(self._update_ui_state_slot)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(5)

        # 标题栏 - 移除渐变背景
        self.header = QFrame()
        self.header.setFixedHeight(35)
        self.header.setStyleSheet("QFrame { background: transparent; border: none; border-bottom: 2px solid #000000; border-top-left-radius: 18px; border-top-right-radius: 18px; border-bottom-left-radius: 0px; border-bottom-right-radius: 0px; }")
        header_layout = QHBoxLayout(self.header)
        header_layout.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        header_layout.setContentsMargins(13, 0, 0, 0)
        header_layout.setSpacing(0)
        lbl_title = QLabel("iMessage")
        lbl_title.setStyleSheet(
            "border: none; color: #2F2F2F; font-family: 'Comic Sans MS', 'Yuanti SC', 'STHeiti'; font-weight: bold; font-size: 15px; padding: 0px;"
        )
        header_layout.addWidget(lbl_title)
        layout.addWidget(self.header)

        # 内容区域 - 统一边距 8, 8, 8, 0
        self.layout = QVBoxLayout()
        self.layout.setContentsMargins(5, 8, 5, 8)
        self.layout.setSpacing(0)
        layout.addLayout(self.layout)

        # 1. 上半部分：左右两个输入框
        top_area = QHBoxLayout()

        # 左框 - 发送号码
        box_l = QFrame()
        box_l.setStyleSheet(
            "QFrame { background: transparent; border: 2px solid #000000; border-radius: 10px; }"
        )
        l_layout = QVBoxLayout(box_l)
        l_layout.setContentsMargins(2, 2, 2, 2)
        l_layout.setSpacing(2)

        # 号码框标题和按钮行
        l_header = QHBoxLayout()
        l_header.setContentsMargins(0, 0, 0, 0)
        l_header.setSpacing(2)
        l_header.addWidget(
            QLabel("发送号码", styleSheet="border:none; color: #2F2F2F; font-family: 'Comic Sans MS', 'Yuanti SC', 'STHeiti'; font-weight: bold;")
        )
        l_header.addStretch()

        # 号码框上按钮
        self.btn_import_recv = QPushButton("📂")
        self.btn_import_recv.setFixedSize(40, 30)
        self.btn_import_recv.setStyleSheet(
            "QPushButton { border: none; background: transparent; border-radius: 12px; font-family: 'Comic Sans MS', 'Yuanti SC', 'STHeiti'; font-weight: bold; } QPushButton:hover { background: rgba(139, 0, 255, 0.2); border-radius: 12px; margin-top: 2px; margin-left: 2px; } QPushButton:pressed { background: rgba(139, 0, 255, 0.3); border-radius: 12px; }"
        )
        self.btn_import_recv.clicked.connect(self.import_numbers_file)
        l_header.addWidget(self.btn_import_recv)

        self.btn_clear_recv = QPushButton("🗑️")
        self.btn_clear_recv.setFixedSize(40, 30)
        self.btn_clear_recv.setStyleSheet(
            "QPushButton { border: none; background: transparent; border-radius: 12px; font-family: 'Comic Sans MS', 'Yuanti SC', 'STHeiti'; font-weight: bold; } QPushButton:hover { background: rgba(255, 0, 0, 0.2); border-radius: 12px; margin-top: 2px; margin-left: 2px; } QPushButton:pressed { background: rgba(255, 0, 0, 0.3); border-radius: 12px; }"
        )
        self.btn_clear_recv.clicked.connect(lambda: self.recv_text.clear())
        l_header.addWidget(self.btn_clear_recv)

        l_layout.addLayout(l_header)

        # 号码输入框（带计数器）
        self.recv_text = TextEditWithCounter(
            "每个号码一行或逗号分隔",
            is_phone_counter=True,
            parent=self,
            placeholder_font_size=8,
        )
        # 设置发送号码输入框样式（白色背景）
        self.recv_text.text_edit.setStyleSheet(
            "QTextEdit { border: 2px solid #999; border-radius: 12px; background-color: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 rgba(240, 250, 255, 0.70), stop:0.45 rgba(255, 240, 250, 0.68), stop:1 rgba(255, 250, 255, 0.70)); color: #2F2F2F; font-family: 'Comic Sans MS', 'Yuanti SC', 'STHeiti'; font-weight: bold; font-size: 12px; padding: 10px; padding: 5px; color: #2F2F2F; } QTextEdit:focus { border: 3px solid #2196F3; background-color: #9df5ed; color: #2F2F2F; }"
        )
        l_layout.addWidget(self.recv_text)

        #  发送内容
        box_r = QFrame()
        box_r.setStyleSheet(
            "QFrame { background: transparent;  border: 2px solid #000000; border-radius: 10px; }"
        )
        r_layout = QVBoxLayout(box_r)
        r_layout.setContentsMargins(2, 2, 2, 2)
        r_layout.setSpacing(2)

        # 内容框标题和按钮行
        r_header = QHBoxLayout()
        r_header.setContentsMargins(0, 0, 0, 0)
        r_header.setSpacing(2)
        r_header.addWidget(
            QLabel("发送内容", styleSheet="border:none; color: #2F2F2F; font-family: 'Comic Sans MS', 'Yuanti SC', 'STHeiti'; font-weight: bold;")
        )
        r_header.addStretch()

        # 内容框上按钮
        self.btn_import_send = QPushButton("📂")
        self.btn_import_send.setFixedSize(40, 30)
        self.btn_import_send.setStyleSheet(
            "QPushButton { border: none; background: transparent; border-radius: 12px; font-family: 'Comic Sans MS', 'Yuanti SC', 'STHeiti'; font-weight: bold; } QPushButton:hover { background: rgba(139, 0, 255, 0.2); border-radius: 12px; margin-top: 2px; margin-left: 2px; } QPushButton:pressed { background: rgba(139, 0, 255, 0.3); border-radius: 12px; }"
        )
        self.btn_import_send.clicked.connect(self.import_message_file)
        r_header.addWidget(self.btn_import_send)

        self.btn_clear_send = QPushButton("🗑️")
        self.btn_clear_send.setFixedSize(40, 30)
        self.btn_clear_send.setStyleSheet(
            "QPushButton { border: none; background: transparent; border-radius: 12px; font-family: 'Comic Sans MS', 'Yuanti SC', 'STHeiti'; font-weight: bold; } QPushButton:hover { background: rgba(255, 0, 0, 0.2); border-radius: 12px; margin-top: 2px; margin-left: 2px; } QPushButton:pressed { background: rgba(255, 0, 0, 0.3); border-radius: 12px; }"
        )
        self.btn_clear_send.clicked.connect(lambda: self.send_text.clear())
        r_header.addWidget(self.btn_clear_send)

        r_layout.addLayout(r_header)

        # 消息输入框（带计数器）
        self.send_text = TextEditWithCounter(
            "请输入短信内容...",
            is_phone_counter=False,
            parent=self,
            placeholder_font_size=8,
        )
        # 设置发送内容输入框样式（白色背景）
        self.send_text.text_edit.setStyleSheet(
            "QTextEdit { border: 2px solid #999; border-radius: 12px; background-color: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 rgba(240, 250, 255, 0.70), stop:0.45 rgba(255, 240, 250, 0.68), stop:1 rgba(255, 250, 255, 0.70)); color: #2F2F2F; font-family: 'Comic Sans MS', 'Yuanti SC', 'STHeiti'; font-weight: bold; font-size: 12px; padding: 10px; padding: 5px; color: #2F2F2F; } QTextEdit:focus { border: 3px solid #2196F3; background-color: #9df5ed; color: #2F2F2F; }"
        )
        r_layout.addWidget(self.send_text)

        # 左侧区域：上下布局（发送号码和发送内容，比例4:3）
        left_area = QVBoxLayout()
        left_area.setSpacing(10)
        left_area.addWidget(box_l, 4)  # 发送号码占4
        left_area.addWidget(box_r, 3)  # 发送内容占3

        # 右侧区域：发送结果
        right_area = QFrame()
        right_area.setStyleSheet(
            "QFrame { background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 rgba(168, 200, 255, 0.22), stop:0.5 rgba(255, 154, 162, 0.18), stop:1 rgba(168, 200, 255, 0.20)); border: 2px solid #000000; border-radius: 10px; }"
        )
        right_layout = QVBoxLayout(right_area)
        right_layout.setContentsMargins(8, 8, 8, 8)

        # 发送结果标题
        result_header = QHBoxLayout()
        result_header.addWidget(
            QLabel(
                "发送结果",
                styleSheet="border:none; font-weight: bold; font-size: 13px; color: #2F2F2F;",
            )
        )
        result_header.addStretch()
        right_layout.addLayout(result_header)

        # 发送结果显示区域
        self.task_status_text = QTextEdit()
        self.task_status_text.setReadOnly(True)
        self.task_status_text.setFocusPolicy(Qt.NoFocus)
        self.task_status_text.setFrameStyle(QTextEdit.NoFrame)
        self.task_status_text.setStyleSheet(
            "QTextEdit { border: none; border-radius: 10px; background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 rgba(240, 250, 255, 0.70), stop:0.45 rgba(255, 240, 250, 0.68), stop:1 rgba(255, 250, 255, 0.70)); color: #2F2F2F; font-family: 'Comic Sans MS', 'Yuanti SC', 'STHeiti'; font-weight: bold; font-size: 12px; padding: 10px; }"
        )
        right_layout.addWidget(self.task_status_text)

        # 主布局：左右分布
        main_content = QHBoxLayout()
        main_content.setSpacing(10)
        main_content.addLayout(left_area, 1)  # 左侧占比1
        main_content.addWidget(right_area, 1)  # 右侧占比1
        self.layout.addLayout(main_content, 1)  # 主内容区域占高度比例 1

        # 2. 发送控制条
        ctrl_row = QHBoxLayout()
        ctrl_row.setSpacing(2)
        ctrl_row.setContentsMargins(5, 5, 5, 5)

        # 统计bar (左边)
        self.global_stats_label = QLabel("🌈总数: 0 |成功: 0 |失败: 0 |成功率: 0%")
        self.global_stats_label.setStyleSheet(
            "border: none; background: transparent; padding: 0; font-size: 11px;font-weight:bold ; color: #2F2F2F;"
        )
        ctrl_row.addWidget(self.global_stats_label)

        ctrl_row.addStretch()
        ctrl_row.addWidget(
            QLabel(
                "发送间隔:",
                styleSheet="border: none; background: transparent; color: #2F2F2F; font-family: 'Comic Sans MS', 'Yuanti SC', 'STHeiti'; font-weight: bold;",
            )
        )

        # 间隔选择框 - 宽度缩小到只能显示X.X
        self.interval_input = QLineEdit()
        self.interval_input.setFixedSize(50, 25)
        self.interval_input.setAlignment(Qt.AlignCenter)
        self.interval_input.setText("1.0")
        self.interval_input.setReadOnly(True)
        self.interval_input.setStyleSheet(
            """
            QLineEdit {
            background: #e281f7;
            border-radius: 12px;
            border: 2px solid #2F2F2F;
            padding: 0px;
            color: #2F2F2F;
            outline: none;
        }
        """
        )
        ctrl_row.addWidget(self.interval_input)

        # 下拉按钮
        self.btn_interval_dropdown = QPushButton("▼")
        self.btn_interval_dropdown.setFixedSize(25, 25)
        self.btn_interval_dropdown.setStyleSheet(
            "QPushButton { border: 2px solid #2F2F2F; border-radius: 12px; background: #c307ed; }"
            "QPushButton:hover { border-radius: 12px; background: rgba(139, 0, 255, 0.2); }"
            "QPushButton:pressed { border-radius: 12px; background: rgba(139, 0, 255, 0.3); }"
        )
        self.btn_interval_dropdown.clicked.connect(self.show_interval_menu)
        ctrl_row.addWidget(self.btn_interval_dropdown)

        # 开始按钮 - 按时样式
        self.btn_send = QPushButton("Send")
        self.btn_send.setFixedHeight(30)
        self.btn_send.setFixedWidth(60)
        self.btn_send.clicked.connect(self.start_sending)
        self.btn_send.setStyleSheet(
            """
            QPushButton {
                border-radius: 12px;
                padding-left: 3px;
                padding-right: 3px;
                font-weight: bold;
                background-color: #FFFACD;
                border: 2px solid #000000;
            }
            QPushButton:hover {
                background-color: #FFFFE0;
                border: 1px solid #FFA500;
            }
            QPushButton:pressed {
                background-color: #FFE4B5;
                border: 1px solid #FF8C00;
            }
        """
        )

        ctrl_row.addWidget(self.btn_send)

        self.layout.addLayout(ctrl_row)

        # 初始化间隔选择菜单
        self.init_interval_menu()

        # 初始化UI状态
        self.update_ui_state()
        self.load_autosave_config()

        # 初始化全局统计显示
        self.update_global_stats()

    def init_interval_menu(self):
        """初始化间隔选择菜单"""
        self.interval_menu = QListWidget()
        self.interval_menu.setWindowFlags(Qt.Popup)
       # self.interval_menu.setAttribute(Qt.WA_TranslucentBackground)
        self.interval_menu.setFixedWidth(80)
        self.interval_menu.setStyleSheet(
            """
            QListWidget {
                background-color: #e281f7;
                border: 2px solid #8B00FF;
                border-radius: 12px;
                outline: none;
                background-clip: border; 
            }
            QListWidget::item {
                padding: 5px;                
                border-radius: 12px;
                color: #2F2F2F;
            }

            QListWidget::item:selected {
                background-color: #8B00FF;
                color: white;
            }
            QListWidget::item:hover {
                background-color: #E6D9FF;
            }
            QListWidget::indicator {
                display: none;
            }
        """
        )

        # 添加间隔选项
        intervals = ["0.3", "0.5", "1.0", "1.5", "2.0"]
        for interval in intervals:
            item = QListWidgetItem(f"{interval}s")
            self.interval_menu.addItem(item)

        self.interval_menu.itemClicked.connect(self.on_interval_selected)
        self.interval_menu.setFocusPolicy(Qt.StrongFocus)
        self.interval_menu.setAutoFillBackground(False)

    def eventFilter(self, obj, event):
        """事件过滤器 - 处理点击外部关闭间隔菜单"""
        if hasattr(self, "interval_menu") and self.interval_menu.isVisible():
            # ESC键关闭
            if event.type() == QEvent.KeyPress and event.key() == Qt.Key_Escape:
                self.interval_menu.hide()
                QApplication.instance().removeEventFilter(self)
                return True

            # 鼠标点击
            if event.type() == QEvent.MouseButtonPress:
                global_pos = event.globalPos()

                # 计算菜单和按钮的全局矩形
                menu_global_rect = QRect(
                    self.interval_menu.mapToGlobal(QPoint(0, 0)),
                    self.interval_menu.size(),
                )
                btn_global_rect = QRect(
                    self.btn_interval_dropdown.mapToGlobal(QPoint(0, 0)),
                    self.btn_interval_dropdown.size(),
                )

                # 如果点击不在菜单和按钮范围内，关闭菜单
                if not menu_global_rect.contains(
                    global_pos
                ) and not btn_global_rect.contains(global_pos):
                    self.interval_menu.hide()
                    QApplication.instance().removeEventFilter(self)
                    return False

        return False

    def show_interval_menu(self):
        pos = self.btn_interval_dropdown.mapToGlobal(
            QPoint(0, self.btn_interval_dropdown.height())
        )

        row_height = self.interval_menu.sizeHintForRow(0)
        rows = self.interval_menu.count()
        spacing = self.interval_menu.spacing()
        frame = self.interval_menu.frameWidth() * 2

        height = rows * row_height + (rows - 1) * spacing + frame
        self.interval_menu.setFixedHeight(height)
        self.interval_menu.move(pos)
        self.interval_menu.show()

        path = QPainterPath()
        path.addRoundedRect(QRectF(self.interval_menu.rect()), 12, 12)
        region = QRegion(path.toFillPolygon().toPolygon())
        self.interval_menu.setMask(region)

        QApplication.instance().installEventFilter(self)

    def on_interval_selected(self, item):
        """间隔选项被选择"""
        interval_text = item.text().replace("s", "")  # 移除's'后缀
        self.interval_input.setText(interval_text)
        self.interval_menu.hide()
        # 移除事件过滤器
        QApplication.instance().removeEventFilter(self)

    def get_phone_numbers(self):
        """获取电话号码列表 - 独立解析，不依赖后端"""
        text = self.recv_text.toPlainText().strip()
        numbers = []
        for line in text.split("\n"):
            if "," in line:
                parts = [n.strip() for n in line.split(",") if n.strip()]
            else:
                parts = [line.strip()] if line.strip() else []

            for num in parts:
                # 如果是10位数字，自动添加+1
                if num.isdigit() and len(num) == 10:
                    num = f"+1{num}"
                if num:
                    numbers.append(num)
        return numbers

    def get_message_content(self):
        return self.send_text.toPlainText().strip()

    def update_ui_state(self):
        """更新UI状态 - 线程安全版本"""
        self.update_ui_state_signal.emit()

    def _update_ui_state_slot(self):
        """更新UI状态的槽函数 - 实际更新UI"""
        self.btn_send.setEnabled(not self.sending)

    def start_sending(self):
        """开始发送 - GUI独立发送，使用AppleScript"""
        if self.sending:
            self._log_to_backend("INFO", 4397, "start_sending", "发送任务已在运行中")
            return

        phones = self.get_phone_numbers()
        message = self.get_message_content()
        interval = float(self.interval_input.text() or "1.0")

        if not phones or not message:
            self._log_to_backend("ERRO", 4404, "start_sending", "号码或内容为空，无法发送")
            return

        self.sending = True
        self.update_ui_state()
        self._log_to_backend("INFO", 4409, "start_sending", f"开始发送任务: {len(phones)}个号码，间隔{interval}秒")

        def send_messages():
            success = 0
            failed = 0
            send_records = []

            try:
                start_time = time.time()
                self._log_to_backend("INFO", 4420, "send_messages", f"开始批量发送 {len(phones)} 条消息")

                for idx, phone in enumerate(phones, 1):
                    if not self.sending:
                        self._log_to_backend("INFO", 4424, "send_messages", "发送已停止")
                        break

                    try:
                        send_time = time.time()
                        script = f"""
                        tell application "Messages"
                            set targetService to 1st account whose service type = iMessage and enabled = true
                            set targetBuddy to participant "{phone}" of targetService
                            send "{message}" to targetBuddy
                        end tell
                        """
                        result = subprocess.run(
                            ["osascript", "-e", script],
                            capture_output=True,
                            text=True,
                            timeout=10,
                        )

                        if result.returncode == 0:
                            send_records.append((phone, send_time, True))
                        else:
                            send_records.append((phone, send_time, False))

                        if idx < len(phones) and self.sending:
                            time.sleep(interval)

                    except Exception as e:
                        send_records.append((phone, time.time(), False))

                real_send_duration = time.time() - start_time
                display_duration = real_send_duration * 0.8
                self._log_to_backend("INFO", 4465, "send_messages", f"发送完成，用时: {display_duration:.1f}秒")

                time.sleep(2)

                for idx, (phone, send_time, script_ok) in enumerate(send_records, 1):
                    if not script_ok:
                        failed += 1
                        continue

                    is_success, status_desc = self.check_actual_message_status(
                        phone, message, send_time
                    )

                    if is_success:
                        success += 1
                    else:
                        failed += 1

                self.task_status_log(f"成功: {success} | 失败: {failed}")
                self._log_to_backend("INFO", 4489, "send_messages", f"结果统计: 成功 {success}, 失败 {failed}")

                self.global_stats["task_count"] += 1
                self.global_stats["total_success"] += success
                self.global_stats["total_fail"] += failed
                self.update_global_stats()

            finally:
                self.sending = False
                self.update_ui_state()

        threading.Thread(target=send_messages, daemon=True).start()

    def import_numbers_file(self):
        fname, _ = QFileDialog.getOpenFileName(
            self, "选择号码文件", "", "文本文件 (*.txt);;所有文件 (*)"
        )
        if fname:
            with open(fname, "r", encoding="utf-8") as f:
                self.recv_text.setText(f.read())
            self._log_to_backend("INFO", 4493, "import_numbers_file", f"导入号码文件: {os.path.basename(fname)}")

    def import_message_file(self):
        fname, _ = QFileDialog.getOpenFileName(
            self, "选择消息文件", "", "文本文件 (*.txt);;所有文件 (*)"
        )
        if fname:
            with open(fname, "r", encoding="utf-8") as f:
                self.send_text.setText(f.read())
            self._log_to_backend("INFO", 4501, "import_message_file", f"导入消息文件: {os.path.basename(fname)}")

    def load_autosave_config(self):
        """加载自动保存的配置"""
        try:
            if os.path.exists(self.config_file):
                # 检查文件是否为空
                file_size = os.path.getsize(self.config_file)
                if file_size == 0:
                    # 文件为空，静默处理，不显示错误
                    return

                with open(self.config_file, "r", encoding="utf-8") as f:
                    content = f.read().strip()
                    if not content:
                        # 文件内容为空，静默处理
                        return

                    data = json.loads(content)
                    last_recv_data = data.get("last_recv_data", "")
                    last_send_data = data.get("last_send_data", "")

                    # 加载到界面
                    if last_recv_data:
                        self.recv_text.setText(last_recv_data)
                    if last_send_data:
                        self.send_text.setText(last_send_data)
        except json.JSONDecodeError:
            # JSON格式错误，静默处理，不显示错误（可能是文件损坏，下次保存时会覆盖）
            pass
        except Exception as e:
            # 其他错误才显示（如权限问题等）
            self.task_status_log(f"加载自动保存配置失败: {str(e)}")

    def save_autosave_config(self):
        """保存自动保存的配置"""
        try:
            data = {
                "last_recv_data": self.recv_text.toPlainText(),
                "last_send_data": self.send_text.toPlainText(),
            }
            with open(self.config_file, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            self.task_status_log(f"保存自动保存配置失败: {str(e)}")

    def task_status_log(self, msg):
        """任务状态显示 - 线程安全版本，通过信号发送"""
        self.task_log_signal.emit(msg)

    def _task_status_log_slot(self, msg):
        """任务状态显示的槽函数 - 只更新发送结果区域"""
        timestamp = datetime.now().strftime("%H:%M")
        if "开始发送" in msg:
            self.task_status_text.clear()
        current_text = self.task_status_text.toPlainText()
        new_text = f"[{timestamp}] {msg}"
        if current_text:
            self.task_status_text.setPlainText(current_text + "\n" + new_text)
        else:
            self.task_status_text.setPlainText(new_text)
        scrollbar = self.task_status_text.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())

    def _log_to_backend(self, level, line_no, func_name, message):
        """本地日志 - 写入文件并显示到PanelBackend日志面板"""
        log(f"[{now_iso()}][WORKER][{level}][{line_no}][{func_name}][{message}]")
        try:
            if hasattr(self, "main_window") and self.main_window:
                if hasattr(self.main_window, "panel_backend"):
                    backend = self.main_window.panel_backend
                    if hasattr(backend, "log_text"):
                        timestamp = datetime.now().strftime("%H:%M")
                        backend_text = backend.log_text.toPlainText()
                        backend_new = f"[{timestamp}][{level}][LOCAL] {message}"
                        if backend_text:
                            backend.log_text.setPlainText(backend_text + "\n" + backend_new)
                        else:
                            backend.log_text.setPlainText(backend_new)
                        sb = backend.log_text.verticalScrollBar()
                        sb.setValue(sb.maximum())
        except:
            pass

    def update_global_stats(self):
        """更新全局统计 - 线程安全版本"""
        self.update_stats_signal.emit(
            self.global_stats["task_count"],
            self.global_stats["total_success"],
            self.global_stats["total_fail"],
        )

    def _update_stats_slot(self, task_count, total_success, total_fail):
        """更新全局统计的槽函数 - 实际更新UI"""
        total = total_success + total_fail
        success_rate = (total_success / total * 100) if total > 0 else 0
        self.global_stats_label.setText(
            f"🌈任务:{task_count}|总数:{total}|"
            f"成功:{total_success}|失败:{total_fail}|"
            f"成功率:{success_rate:.1f}%"
        )

    def check_actual_message_status(self, phone, message, min_time=None):
        db_path_str = str(Path.home() / "Library" / "Messages" / "chat.db")
        try:
            conn = sqlite3.connect(db_path_str, timeout=5.0)
        except sqlite3.OperationalError as e:
            error_msg = str(e).lower()
            if "unable to open" in error_msg:
                return (
                    False,
                    "无法打开数据库：请在'系统设置 → 隐私与安全性 → 完全磁盘访问权限'中添加此应用",
                )
            elif "database is locked" in error_msg:
                return False, "数据库被锁定：请关闭'信息'应用后重试"
            else:
                return False, f"数据库错误: {e}"
        except FileNotFoundError:
            return False, "数据库文件不存在：请至少发送/接收一条iMessage以创建数据库"

        try:
            cursor = conn.cursor()

            # 计算时间戳（放宽10分钟缓冲，确保能找到记录）
            min_date_ns = 0
            if min_time:
                min_date_ns = int((min_time - 600 - 978307200) * 1000000000)

            # 查询最近发送的消息
            query = """
            SELECT m.ROWID, m.error, m.date_read, m.date_delivered, m.text, m.date
            FROM message m
            JOIN handle h ON m.handle_id = h.ROWID
            WHERE m.is_from_me = 1
            AND (h.id = ? OR h.id = ?) 
            AND m.date >= ?
            ORDER BY m.date DESC
            LIMIT 1
            """

            phone_alt = (
                phone.replace("+1", "") if phone.startswith("+1") else f"+1{phone}"
            )

            cursor.execute(query, (phone, phone_alt, min_date_ns))
            row = cursor.fetchone()

            if row:
                rowid, error_code, date_read, date_delivered, db_text, db_date = row

                if error_code == 0:
                    final_status = "发送成功"
                    if date_read > 0:
                        final_status += " (已读)"
                    elif date_delivered > 0:
                        final_status += " (已送达)"
                    return True, final_status
                else:
                    return False, f"发送失败 (错误码: {error_code})"
            else:
                return False, "未找到记录 (号码或时间不匹配)"
        finally:
            # 确保连接始终被关闭
            conn.close()

    def closeEvent(self, event):
        """关闭时保存配置"""
        self.save_autosave_config()
        super().closeEvent(event)

class PanelInbox(FixedSizePanel):

    _LOCAL_DB_PATH = os.path.expanduser("~/Library/Messages/chat.db")
    #查找本地 Messages 数据库路径
    @staticmethod
    def _find_local_database():
        import platform

        if platform.system() != "Darwin":
            return None

        possible_paths = [
            os.path.expanduser("~/Library/Messages/chat.db"),
            os.path.expanduser(
                "~/Library/Containers/com.apple.iChat/Data/Library/Messages/chat.db"
            ),
        ]

        for p in possible_paths:
            try:
                if os.path.exists(p) and os.path.getsize(p) > 0:
                    return p
            except:
                continue
        return None
    @staticmethod
    def _check_imessage_logged_in():
        """检查本地 iMessage 是否已登录（完全本地检测）"""
        import platform

        if platform.system() != "Darwin":
            return False

        db_path = PanelInbox._find_local_database()
        if not db_path:
            return False

        try:
            conn = sqlite3.connect(db_path, timeout=3.0)
            cursor = conn.cursor()
            # 检查 account 表是否存在且有 iMessage 记录
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='account'"
            )
            if not cursor.fetchone():
                conn.close()
                return False
            cursor.execute(
                "SELECT account_login FROM account WHERE service_name LIKE '%iMessage%' LIMIT 1"
            )
            result = cursor.fetchone()
            conn.close()
            return result is not None
        except:
            return False
    #解码 attributedBody 
    @staticmethod
    def _decode_attributed_body(blob):
        if not blob:
            return None
        try:
            attributed_body = blob.decode("utf-8", errors="replace")
            if "NSNumber" in attributed_body:
                attributed_body = attributed_body.split("NSNumber")[0]
            if "NSString" in attributed_body:
                attributed_body = attributed_body.split("NSString")[1]
            if "NSDictionary" in attributed_body:
                attributed_body = attributed_body.split("NSDictionary")[0]
            if len(attributed_body) > 18:
                attributed_body = attributed_body[6:-12]
            else:
                attributed_body = attributed_body[6:]
            body = attributed_body.strip()
            if body and not body.isprintable():
                body = "".join(c for c in body if c.isprintable() or c in "\n\t ")
            return body if body else None
        except:
            return None
    #时间戳解析辅助函数
    @staticmethod
    def _get_timestamp_for_sort(msg_timestamp):
        dt = datetime.fromisoformat(msg_timestamp)
        if dt.tzinfo is not None:
            dt = dt.astimezone().replace(tzinfo=None)
        return dt

    def __init__(self, parent_window):
        gradient_bg = "qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #bfe3ff, stop:0.5 #d6eeff, stop:1 #eef7ff);"

        super().__init__(gradient_bg, 550, 430, parent_window)
        self.main_window = parent_window

        # 收件箱相关数据
        self.max_rowid = 0
        self.chats_data = {}
        self.inbox_checker_thread = None
        self.inbox_checker_running = False
        self.current_chat_id = None

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        # 标题栏 - 移除渐变背景
        self.header = QFrame()
        self.header.setFixedHeight(35)
        self.header.setStyleSheet("QFrame { background: transparent; border: none; border-bottom: 2px solid #000000; border-top-left-radius: 18px; border-top-right-radius: 18px; border-bottom-left-radius: 0px; border-bottom-right-radius: 0px; }")
        header_layout = QHBoxLayout(self.header)
        header_layout.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        header_layout.setContentsMargins(13, 0, 0, 0)
        header_layout.setSpacing(0)
        lbl_title = QLabel("收件箱")
        lbl_title.setStyleSheet(
            "border: none; color: #2F2F2F; font-family: 'Comic Sans MS', 'Yuanti SC', 'STHeiti'; font-weight: bold; font-size: 15px; padding: 0px;"
        )
        header_layout.addWidget(lbl_title)
        header_layout.addStretch()
        layout.addWidget(self.header)

        # 内容区域 - 统一边距 8, 8, 8, 0
        content_area = QHBoxLayout()
        content_area.setContentsMargins(8, 8, 8, 0)
        content_area.setSpacing(10)

        # 左侧：联系人列表（参考前端样式）
        left_panel = QFrame()
        left_panel.setStyleSheet(
            "QFrame { background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 rgba(168, 200, 255, 0.25), stop:0.5 rgba(255, 154, 162, 0.20), stop:1 rgba(168, 200, 255, 0.22)); border: 2px solid #000000; border-radius: 10px; }"
        )
        left_panel.setFixedWidth(250)
        left_layout = QVBoxLayout(left_panel)
        left_layout.setContentsMargins(8, 8, 8, 8)
        left_layout.setSpacing(8)

        # 收件箱标题
        inbox_header = QHBoxLayout()
        inbox_header.addWidget(
            QLabel(
                "📨 收件箱",
                styleSheet="border:none; font-weight: bold; font-size: 14px; color: #2F2F2F;",
            )
        )
        inbox_header.addStretch()
        left_layout.addLayout(inbox_header)

        # 收件箱列表（参考前端样式）
        self.inbox_list = QListWidget()
        self.inbox_list.setStyleSheet(
            """
            QListWidget {
                border: none;
                border-radius: 10px;
                background: transparent;
                font-family: 'Comic Sans MS', 'Yuanti SC', 'STHeiti'; font-weight: bold;
                font-size: 12px;
                color: #2F2F2F;
                padding: 6px;
            }
            QListWidget::item {
                padding: 10px;
                border-radius: 10px;
                margin-bottom: 8px;
                background: rgba(255, 255, 255, 0.60);
                color: #2F2F2F;
                min-height: 50px;
            }
            QListWidget::item:hover {
                background: rgba(255, 255, 255, 0.80);
            }
            QListWidget::item:selected {
                background: rgba(255, 255, 255, 0.95);
                border: 3px solid #000000;
            }
        """
        )
        self.inbox_list.itemClicked.connect(self.on_inbox_item_clicked)
        left_layout.addWidget(self.inbox_list)

        content_area.addWidget(left_panel)

        # 右侧：对话显示区域（参考前端样式）
        right_panel = QFrame()
        right_panel.setStyleSheet("background: transparent; border: none;")
        right_layout = QVBoxLayout(right_panel)
        right_layout.setContentsMargins(0, 0, 0, 0)
        right_layout.setSpacing(6)

        # 对话标题（参考前端样式）
        self.conversation_title = QLabel("选择一个对话")
        self.conversation_title.setStyleSheet(
            """
            QLabel {
                border: 2px solid #000000;
                border-radius: 10px;
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 rgba(168, 200, 255, 0.20), stop:0.5 rgba(255, 154, 162, 0.18), stop:1 rgba(168, 200, 255, 0.19));
                padding: 10px;
                font-weight: bold;
                font-size: 14px;
                color: #2F2F2F;
                font-family: 'Comic Sans MS', 'Yuanti SC', 'STHeiti'; font-weight: bold;
            }
        """
        )
        right_layout.addWidget(self.conversation_title)

        # 对话显示区域（参考前端样式）
        self.conversation_display = QTextEdit()
        self.conversation_display.setReadOnly(True)
        self.conversation_display.setStyleSheet(
            "QTextEdit { border: 2px solid #000000; border-radius: 10px; background: rgba(255, 255, 255, 0.30); color: #2F2F2F; font-family: 'Comic Sans MS', 'Yuanti SC', 'STHeiti'; font-weight: bold; font-size: 12px; padding: 10px; }"
        )
        right_layout.addWidget(self.conversation_display, 1)

        # 回复输入区域（参考前端样式）
        reply_row = QHBoxLayout()
        reply_row.setSpacing(8)
        reply_row.setContentsMargins(0, 0, 0, 2)
        self.reply_input = QLineEdit()
        self.reply_input.setPlaceholderText("输入回复...")
        self.reply_input.setStyleSheet(
            """
            QLineEdit {
                border: 2px solid #000000;
                border-radius: 18px;
                padding: 8px 12px;
                font-size: 13px;
                color: #2F2F2F;
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 rgba(168, 200, 255, 0.30), stop:0.45 rgba(255, 154, 162, 0.28), stop:1 rgba(255, 179, 186, 0.29));
                font-family: 'Comic Sans MS', 'Yuanti SC', 'STHeiti'; font-weight: bold;
            }
            QLineEdit:focus {
                border-color: #2196F3;
            }
        """
        )
        self.reply_input.setEnabled(False)
        self.reply_btn = QPushButton("发送")
        self.reply_btn.setFixedWidth(80)
        self.reply_btn.setEnabled(False)
        self.reply_btn.setStyleSheet(
            """
            QPushButton {
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #ffecd2, stop:0.5 #fcb69f, stop:1 #ffb347);
                color: #2F2F2F;
                border: 2px solid #000000;
                border-radius: 12px;
                padding: 8px 24px;
                font-weight: bold;
                font-size: 13px;
                font-family: 'Comic Sans MS', 'Yuanti SC', 'STHeiti'; font-weight: bold;
            }
            QPushButton:hover:enabled {
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #d0fcc4, stop:0.5 #2eef68, stop:1 #02ff0a);
                border-radius: 12px;
                margin-top: 1px;
                margin-left: 1px;
            }
            QPushButton:pressed:enabled {
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #f50bce, stop:0.5 #ff1f70, stop:1 #ff6b35);
                border-radius: 12px;
            }
            QPushButton:disabled {
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #ffecd2, stop:0.5 #fcb69f, stop:1 #ffb347);
                border-radius: 12px;
                color: #666;
            }
        """
        )
        self.reply_btn.clicked.connect(self.send_reply)
        self.reply_input.returnPressed.connect(self.send_reply)
        reply_row.addWidget(self.reply_input)
        reply_row.addWidget(self.reply_btn)
        right_layout.addLayout(reply_row)

        content_area.addWidget(right_panel, 1)

        layout.addLayout(content_area)

        # 初始化收件箱
        self.start_inbox_checker()
    # 发送iMessage消息 (本地)
    def send_message(self, phone, message):

        try:
            # 使用AppleScript发送消息
            script = f"""
            tell application "Messages"
                set targetService to 1st account whose service type = iMessage
                set targetBuddy to participant "{phone}" of targetService
                send "{message}" to targetBuddy
            end tell
            """
            result = subprocess.run(
                ["osascript", "-e", script], capture_output=True, text=True, timeout=10
            )

            if result.returncode == 0:
                log(f"[{now_iso()}][WORKER][INFO][4989][send_message][消息已发送到 {phone}]")
                return True
            else:
                return False
        except Exception as e:
            log(f"[{now_iso()}][WORKER][ERRO][4995][send_message][发送消息出错: {str(e)}]")
            return False
    # 初始化收件箱 -(只加载一次数据)
    def start_inbox_checker(self):
        # 使用类内部方法检查 iMessage 登录状态
        if not self._check_imessage_logged_in():
            # 静默处理，本地GUI不需要报错
            return

        # 验证数据库是否可用
        db_check = self._check_database_available()
        if not db_check["available"]:
            return

        # 获取数据库路径
        self._db_path = db_check.get("db_path")

        # 初始化加载一次数据
        self._load_initial_data()
    #检查 Messages 数据库是否可用 (本地)
    def _check_database_available(self):
        # 使用类内部方法查找数据库
        local_db_path = self._find_local_database()

        # 文件不存在：静默返回，不报错
        if not local_db_path:
            return {
                "available": False,
                "reason": "",  # 静默
                "is_permission_issue": False,
            }

        # 尝试打开数据库
        try:
            conn = sqlite3.connect(local_db_path, timeout=3.0)
            cursor = conn.cursor()
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='message'"
            )
            result = cursor.fetchone()
            conn.close()

            if not result:
                return {
                    "available": False,
                    "reason": "",  # 静默
                    "is_permission_issue": False,
                }

            return {"available": True, "reason": "", "db_path": local_db_path}
        except sqlite3.OperationalError as e:
            error_msg = str(e).lower()
            if "unable to open database file" in error_msg:
                # 权限问题：输出详细的获取权限方法
                return {
                    "available": False,
                    "is_permission_issue": True,
                    "reason": (
                        "⚠️ 无法访问 Messages 数据库，需要授予「完全磁盘访问权限」\n"
                        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                        "📝 获取权限的步骤：\n"
                        "   1. 打开「系统设置」(System Settings)\n"
                        "   2. 点击「隐私与安全性」(Privacy & Security)\n"
                        "   3. 在左侧列表中选择「完全磁盘访问权限」(Full Disk Access)\n"
                        "   4. 点击右下角的 🔒 锁图标并输入密码解锁\n"
                        "   5. 点击 ➕ 按钮，添加此应用程序\n"
                        "   6. 重启应用程序\n"
                        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
                    ),
                }
            else:
                # 其他数据库错误：静默
                return {
                    "available": False,
                    "reason": "",
                    "is_permission_issue": False,
                }
        except Exception:
            return {
                "available": False,
                "reason": "",
                "is_permission_issue": False,
            }
    #加载最近的聊天记录
    def _load_initial_data(self):
        """初始化加载数据 - """
        if not hasattr(self, "_db_path") or not self._db_path:
            return

        try:
            conn = sqlite3.connect(self._db_path, timeout=5.0)
            cursor = conn.cursor()

            # 获取最大 ROWID
            cursor.execute("SELECT MAX(ROWID) FROM message")
            self.max_rowid = cursor.fetchone()[0] or 0

            # 加载最近 100 条消息用于初始显示
            query = """
            SELECT 
                COALESCE(chat.chat_identifier, handle.id) AS chat_identifier,
                COALESCE(chat.display_name, handle.id) AS chat_name,
                message.ROWID,
                message.text,
                message.attributedBody,
                message.is_from_me,
                message.date,
                handle.id as sender_id
            FROM message
            LEFT JOIN chat_message_join ON message.ROWID = chat_message_join.message_id
            LEFT JOIN chat ON chat_message_join.chat_id = chat.ROWID
            LEFT JOIN handle ON message.handle_id = handle.ROWID
            ORDER BY message.date DESC
            LIMIT 100
            """
            cursor.execute(query)
            rows = cursor.fetchall()
            conn.close()

            # 处理消息数据
            self._process_message_rows(rows)

            # 更新 UI
            self.update_inbox_list()

        except Exception:
            # 静默处理错误
            pass
    #刷新新消息 - 打开面板或发送消息后调用
    def _refresh_new_messages(self):

        if not hasattr(self, "_db_path") or not self._db_path:
            return

        try:
            conn = sqlite3.connect(self._db_path, timeout=5.0)
            cursor = conn.cursor()

            # 查询新消息
            query = """
            SELECT 
                COALESCE(chat.chat_identifier, handle.id) AS chat_identifier,
                COALESCE(chat.display_name, handle.id) AS chat_name,
                message.ROWID,
                message.text,
                message.attributedBody,
                message.is_from_me,
                message.date,
                handle.id as sender_id
            FROM message
            LEFT JOIN chat_message_join ON message.ROWID = chat_message_join.message_id
            LEFT JOIN chat ON chat_message_join.chat_id = chat.ROWID
            LEFT JOIN handle ON message.handle_id = handle.ROWID
            WHERE message.ROWID > ?
            ORDER BY message.date
            """
            cursor.execute(query, (self.max_rowid,))
            new_rows = cursor.fetchall()
            conn.close()

            if new_rows:
                # 处理新消息
                self._process_message_rows(new_rows)
                # 更新 UI
                self.update_inbox_list()

        except Exception:
            # 静默处理错误
            pass
    # 处理消息行数据 - 统一的消息处理逻辑
    def _process_message_rows(self, rows):
        for row in rows:
            (
                chat_id,
                display_name,
                rowid,
                text,
                attr_body,
                is_from_me,
                date,
                sender_id,
            ) = row

            # 更新 max_rowid
            self.max_rowid = max(self.max_rowid, rowid)

            # 解码消息文本
            message_text = text or self._decode_attributed_body(attr_body)
            if not message_text:
                continue

            # 计算时间戳
            timestamp = (
                datetime(2001, 1, 1, tzinfo=timezone.utc)
                + timedelta(seconds=date / 1000000000)
                if date
                else datetime.now(timezone.utc)
            ).astimezone()

            # 创建或更新聊天数据
            if chat_id not in self.chats_data:
                final_chat_name = display_name or sender_id or chat_id
                self.chats_data[chat_id] = {
                    "name": final_chat_name,
                    "messages": [],
                }

            message_entry = {
                "text": message_text,
                "is_from_me": bool(is_from_me),
                "timestamp": timestamp.isoformat(),
                "sender": sender_id or "Unknown",
                "rowid": rowid,
            }

            # 避免重复添加
            if not any(
                m.get("rowid") == rowid for m in self.chats_data[chat_id]["messages"]
            ):
                self.chats_data[chat_id]["messages"].append(message_entry)
    # 创建收件人列表
    def get_chatlist(self):

        chat_list = []

        sorted_chats = sorted(
            self.chats_data.items(),
            key=lambda x: (
                self._get_timestamp_for_sort(x[1]["messages"][-1]["timestamp"])
                if x[1]["messages"]
                else datetime.min
            ),
            reverse=True,
        )

        for chat_id, chat in sorted_chats:
            if chat["messages"]:
                last_msg = chat["messages"][-1]
                preview = (
                    last_msg["text"][:35] + "..."
                    if len(last_msg["text"]) > 35
                    else last_msg["text"]
                )
                time_str = datetime.fromisoformat(last_msg["timestamp"]).strftime(
                    "%H:%M"
                )
                chat_list.append(
                    {
                        "chat_id": chat_id,
                        "name": chat["name"],
                        "last_message_preview": preview,
                        "last_message_time": time_str,
                    }
                )
            else:
                chat_list.append(
                    {
                        "chat_id": chat_id,
                        "name": chat["name"],
                        "last_message_preview": "无消息",
                        "last_message_time": "",
                    }
                )
        return chat_list
    # 更新收件箱列表显示
    def update_inbox_list(self):

        chat_list = self.get_chatlist()
        self.inbox_list.clear()

        if not chat_list:
            item = QListWidgetItem("暂无对话")
            item.setData(Qt.UserRole, None)
            self.inbox_list.addItem(item)
            return

        for chat in chat_list:
            if chat["last_message_time"]:
                item_text = f"{chat['name']}\n{chat['last_message_preview']} - {chat['last_message_time']}"
            else:
                item_text = f"{chat['name']}\n{chat['last_message_preview']}"
            item = QListWidgetItem(item_text)
            item.setData(Qt.UserRole, chat["chat_id"])
            self.inbox_list.addItem(item)
    #获取指定对话的所有消息
    def get_conversation(self, chat_id):
        if chat_id not in self.chats_data:
            return None

        chat = self.chats_data[chat_id]
        messages_for_display = []

        sorted_messages = sorted(
            chat["messages"], key=lambda x: self._get_timestamp_for_sort(x["timestamp"])
        )

        for msg in sorted_messages:
            messages_for_display.append(
                {
                    "text": msg["text"],
                    "is_from_me": msg["is_from_me"],
                    "timestamp": datetime.fromisoformat(msg["timestamp"]).strftime(
                        "%H:%M"
                    ),
                }
            )
        return {
            "name": chat["name"],
            "messages": messages_for_display,
        }
    # 收件箱项被点击
    def on_inbox_item_clicked(self, item):
        chat_id = item.data(Qt.UserRole)
        if not chat_id:
            return

        conversation = self.get_conversation(chat_id)
        if not conversation:
            return

        # 显示对话
        self.current_chat_id = chat_id
        self.conversation_title.setText(conversation["name"])
        self.conversation_display.clear()

        for msg in conversation["messages"]:
            if msg["is_from_me"]:
                display_text = f"我: {msg['text']}"
            else:
                display_text = f"{conversation['name']}: {msg['text']}"
            # 使用setPlainText + 追加文本的方式，避免QTextCursor跨线程问题
            current_text = self.conversation_display.toPlainText()
            new_text = f"[{msg['timestamp']}] {display_text}"
            if current_text:
                self.conversation_display.setPlainText(current_text + "\n" + new_text)
            else:
                self.conversation_display.setPlainText(new_text)

        # 滚动到底部
        scrollbar = self.conversation_display.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())

        # 启用回复功能
        self.reply_input.setEnabled(True)
        self.reply_btn.setEnabled(True)
        self.reply_input.setFocus()
    # 发送回复
    def send_reply(self):
        if not hasattr(self, "current_chat_id") or not self.current_chat_id:
            return

        reply_text = self.reply_input.text().strip()
        if not reply_text:
            return

        chat_id = self.current_chat_id

        # 立即显示在对话中
        now = datetime.now()
        # 使用setPlainText + 追加文本的方式，避免QTextCursor跨线程问题
        current_text = self.conversation_display.toPlainText()
        new_text = f"[{now.strftime('%H:%M')}] 我: {reply_text}"
        if current_text:
            self.conversation_display.setPlainText(current_text + "\n" + new_text)
        else:
            self.conversation_display.setPlainText(new_text)
        scrollbar = self.conversation_display.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())

        self.reply_input.clear()

        # 发送消息
        success = self.send_message(chat_id, reply_text)

        # 添加到 chats_data
        if chat_id not in self.chats_data:
            self.chats_data[chat_id] = {
                "name": chat_id,
                "messages": [],
            }

        message_entry = {
            "text": reply_text,
            "is_from_me": True,
            "timestamp": now.isoformat(),
            "sender": "Me",
            "rowid": -int(time.time() * 1000),
        }
        self.chats_data[chat_id]["messages"].append(message_entry)

        # 更新收件箱列表
        self.update_inbox_list()
    # 关闭时停止收件箱检查器
    def closeEvent(self, event):
        self.inbox_checker_running = False
        super().closeEvent(event)

class PanelID(FixedSizePanel):
   
    # region  面板样式inti
    def __init__(self, parent_window):
        gradient_bg = "qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #ffd9a0, stop:0.5 #ffbf70, stop:1 #ffa94d);"
        super().__init__(gradient_bg, 550, 430, parent_window)
        self.main_window = parent_window
        self.config_dir = get_app_data_dir("Autosender Pro")
        os.makedirs(self.config_dir, exist_ok=True)
        self.config_file = os.path.join(self.config_dir, "autologin_config.json")
        self.last_used = None
        self.load_config()
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)
        # 标题栏 - 移除渐变背景，使用透明背景，与输入框标签对齐
        self.header = QFrame()
        self.header.setFixedHeight(35)
        self.header.setStyleSheet("QFrame { background: transparent; border: none; border-bottom: 2px solid #000000; border-top-left-radius: 18px; border-top-right-radius: 18px; border-bottom-left-radius: 0px; border-bottom-right-radius: 0px; }")
        header_layout = QHBoxLayout(self.header)
        header_layout.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        header_layout.setContentsMargins(8, 0, 0, 0)
        header_layout.setSpacing(0)
        # 添加间距使标题与输入框标签左对齐（标签宽度90，右对齐，所以标题从8开始即可）
        lbl_title = QLabel("账号管理")
        lbl_title.setStyleSheet(
            f"border: none; color: #2F2F2F; font-family: 'Comic Sans MS', 'Yuanti SC', 'STHeiti'; font-weight: bold; font-size: 15px; padding: 0px;"
        )
        header_layout.addWidget(lbl_title)
        header_layout.addStretch()
        layout.addWidget(self.header)

        # 内容区域 - 统一边距 8, 8, 8, 0
        self.layout = QVBoxLayout()
        self.layout.setAlignment(Qt.AlignTop)
        self.layout.setContentsMargins(0, 20, 8, 0)
        layout.addLayout(self.layout)

        input_css = "QLineEdit { border: 2px solid #000000; border-radius: 10px; padding: 4px 8px; font-size: 12px; color: #2F2F2F; background: qlineargradient(x1:0, y1:0, x2:1, y2:1,stop:0 #f7d5a3,stop:1 #e8d1ae); }"
        # 修改字体为 Yuanti SC
        label_css = f"border: none; background: transparent; font-family: 'Yuanti SC'; font-weight: bold; font-size: 14px; color: #2F2F2F;"

        # 1. Apple ID 行：标签 + 输入框 + Save按钮（居中显示）
        row1 = QHBoxLayout()
        row1.setAlignment(Qt.AlignCenter)  # 整体居中
        l1 = QLabel("APPLE ID  ")
        l1.setFixedWidth(90)
        l1.setAlignment(Qt.AlignCenter | Qt.AlignVCenter)  # 改为居中
        l1.setStyleSheet(label_css)
        # 确保文字清晰：移除图形效果，设置纯文本格式
        l1.setGraphicsEffect(None)  # 移除可能导致模糊的图形效果
        l1.setTextFormat(Qt.PlainText)  # 使用纯文本格式，避免渲染问题

        self.edit_id = QLineEdit()
        self.edit_id.setFixedSize(220, 35)
        self.edit_id.setStyleSheet(input_css)
        self.edit_id.setFrame(False)

        self.btn_save = QPushButton("保存")
        self.btn_save.setFixedSize(60, 32)
        self.btn_save.setCursor(Qt.PointingHandCursor)
        self.btn_save.setStyleSheet(
            "QPushButton { background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #d0fcc4, stop:0.5 #2eef68, stop:1 #02ff0a); color: #2F2F2F; border: 2px solid #000000; border-radius: 12px; padding: 4px 10px; font-weight: bold; font-size: 12px; font-family: 'Comic Sans MS', 'Yuanti SC', 'STHeiti'; font-weight: bold; } QPushButton:hover:enabled { background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #f2fff0, stop:1 #c5ffc1); margin-top: 1px; margin-left: 1px; } QPushButton:pressed:enabled { background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #a8ffbd, stop:1 #70ff9c); } QPushButton:disabled { background: #ccc; color: #666; }"
        )
        self.btn_save.clicked.connect(self.save_current_account)

        row1.addStretch()
        row1.addWidget(l1)
        row1.addWidget(self.edit_id)
        row1.addSpacing(10)  # 间隔点距离
        row1.addWidget(self.btn_save)
        row1.addStretch()
        self.layout.addLayout(row1)

        self.layout.addSpacing(10)  # 行间距

        row2 = QHBoxLayout()
        row2.setAlignment(Qt.AlignCenter)
        l2 = QLabel("PASSWORD ")
        l2.setFixedWidth(90)
        l2.setAlignment(Qt.AlignCenter | Qt.AlignVCenter)
        l2.setStyleSheet(label_css)

        l2.setGraphicsEffect(None)
        l2.setTextFormat(Qt.PlainText)

        # 密码输入框（与账号输入框宽度一致）
        self.edit_pass = QLineEdit()
        self.edit_pass.setFixedSize(220, 35)
        self.edit_pass.setEchoMode(QLineEdit.Password)
        self.edit_pass.setStyleSheet(
            input_css + "padding-right: 35px;"
        )  # 右侧留空间给按钮
        self.edit_pass.setFrame(False)

        # 密码输入框容器（用于绝对定位按钮）
        pass_container = QWidget()
        pass_container.setFixedSize(220, 35)
        pass_container.setStyleSheet("background: transparent; border: none;")

        # 将输入框作为容器的子控件
        self.edit_pass.setParent(pass_container)
        self.edit_pass.move(0, 0)

        # 显示/隐藏密码按钮（绝对定位在输入框内部右侧）
        self.btn_toggle_pass = QPushButton("👁", pass_container)
        self.btn_toggle_pass.setFixedSize(30, 30)
        self.btn_toggle_pass.setCursor(Qt.PointingHandCursor)
        self.btn_toggle_pass.move(187, 2)  # 220-30-3 = 187, 垂直居中
        self.btn_toggle_pass.setStyleSheet(
            """
            QPushButton {
                background: transparent;
                border: none;
                color: #666;
                font-size: 16px;
            }
            QPushButton:hover {
                color: #2196F3;
            }
        """
        )
        self.btn_toggle_pass.clicked.connect(self.toggle_password_visibility)

        self.btn_login = QPushButton("登录")
        self.btn_login.setFixedSize(60, 32)
        self.btn_login.setCursor(Qt.PointingHandCursor)
        self.btn_login.setStyleSheet(
            "QPushButton { background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #ffecd2, stop:0.5 #fcb69f, stop:1 #ffb347); color: #2F2F2F; border: 2px solid #000000; border-radius: 12px; padding: 4px 10px; font-weight: bold; font-size: 12px; font-family: 'Comic Sans MS', 'Yuanti SC', 'STHeiti'; font-weight: bold; } QPushButton:hover:enabled { background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #ffe9dc, stop:1 #ffd1b1); margin-top: 1px; margin-left: 1px; } QPushButton:pressed:enabled { background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #fcb69f, stop:1 #ffb347); } QPushButton:disabled { background: #ccc; color: #666; }"
        )
        self.btn_login.clicked.connect(self.accept_login)

        row2.addStretch()
        row2.addWidget(l2)
        row2.addWidget(pass_container)
        row2.addSpacing(10)  # 间隔点距离
        row2.addWidget(self.btn_login)
        row2.addStretch()
        self.layout.addLayout(row2)

        self.layout.addSpacing(25)  # 间距

        # === 账号管理边框区域 ===

        # 创建边框框，宽度与输入框行对齐，高度延伸到面板底部
        account_mgmt_frame = QFrame()
        account_mgmt_frame.setFrameShape(QFrame.NoFrame)  # 移除默认边框
        account_mgmt_frame.setFixedWidth(450)  # 固定宽度，与输入框行对齐
        account_mgmt_frame.setStyleSheet(
            f"""
            QFrame {{
                border: none !important;
                outline: none !important;
                border-radius: 10px;
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1,
                    stop:0 rgba(255, 255, 255, 0.18),
                    stop:1 rgba(255, 255, 255, 0.10)
                );
            }}
        """
        )
        account_mgmt_frame_layout = QVBoxLayout(account_mgmt_frame)
        account_mgmt_frame_layout.setContentsMargins(15, 0, 15, 0)
        account_mgmt_frame_layout.setSpacing(8)

        # === 顶部：标题、智能登录按钮和导入按钮 ===
        top_header = QHBoxLayout()
        top_header.setContentsMargins(10, 8, 0, 0)
        title_label = QLabel("账号列表")
        title_label.setStyleSheet(
            f"border: none; background: transparent; font-family: 'Comic Sans MS', 'Yuanti SC', 'STHeiti'; font-weight: bold; font-size: 14px; color: #2F2F2F; font-weight: bold;"
        )
        top_header.addWidget(title_label)
        top_header.addStretch()

        # 加载ID列表按钮
        self.btn_auto_login = QPushButton("加载ID列表")
        self.btn_auto_login.setFixedSize(110, 32)
        self.btn_auto_login.setCursor(Qt.PointingHandCursor)
        self.btn_auto_login.setStyleSheet(
            f"""
            QPushButton {{
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #42A5F5, stop:1 #2196F3);
                color: white;
                border: 2px solid #000000;
                border-radius: 10px;
                font-family: 'Comic Sans MS', 'Yuanti SC', 'STHeiti'; font-weight: bold; font-size: 12px;
                font-weight: bold;
            }}
            QPushButton:hover {{
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #64B5F6, stop:1 #42A5F5);
                border-color: #1976D2;
            }}
            QPushButton:pressed {{
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #2196F3, stop:1 #1565C0);
            }}
        """
        )
        self.btn_auto_login.clicked.connect(self.load_config)
        top_header.addWidget(self.btn_auto_login)

        top_header.addSpacing(10)  # 与导入按钮间隔

        self.btn_import_list = QPushButton("📂")
        self.btn_import_list.setFixedSize(30, 30)
        self.btn_import_list.setCursor(Qt.PointingHandCursor)
        # 导入按钮：无边框、透明背景
        self.btn_import_list.setStyleSheet(
            f"""
            QPushButton {{
                border: none;
                background: transparent;
                color: #2F2F2F;
                font-size: 18px;
            }}
            QPushButton:hover {{
                background: rgba(255, 255, 255, 0.18);
                border-radius: 8px;
            }}
            QPushButton:pressed {{
                background: rgba(255, 255, 255, 0.28);
            }}
        """
        )
        self.btn_import_list.clicked.connect(self.import_accounts_file)
        top_header.addWidget(self.btn_import_list)
        account_mgmt_frame_layout.addLayout(top_header)

        # === 中间：账号列表滚动区域（高度延伸到容器底部） ===
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_area.setFrameShape(QFrame.NoFrame)
        scroll_area.setStyleSheet(
            f"""
            QScrollArea {{ 
                border: none !important; 
                outline: none !important;
                background: transparent !important; 
            }}
            QAbstractScrollArea::viewport {{ 
                border: none !important; 
                outline: none !important;
                background: transparent !important; 
            }}
            QScrollArea > QWidget {{
                border: none !important;
                outline: none !important;
                background: transparent !important;
            }}
        """
        )

        # 账号列表容器（简化：直接作为 scroll_area 的内容，不需要额外 widget）
        self.account_list_widget = QWidget()
        self.account_list_widget.setStyleSheet(
            f"""
            QWidget {{ 
                border: none !important; 
                outline: none !important; 
                background: transparent !important; 
            }}
            QWidget * {{
                border: none !important;
                outline: none !important;
            }}
        """
        )
        self.account_list_layout = QVBoxLayout(self.account_list_widget)
        self.account_list_layout.setContentsMargins(10, 6, 10, 6)
        self.account_list_layout.setSpacing(4)
        self.account_list_layout.setAlignment(Qt.AlignTop)

        scroll_area.setWidget(self.account_list_widget)
        account_mgmt_frame_layout.addWidget(
            scroll_area, 1
        )  # 使用stretch让列表区域占据剩余空间，延伸到容器底部

        # === 底部：全部删除按钮 ===
        bottom_footer = QHBoxLayout()
        bottom_footer.setContentsMargins(10, 0, 10, 10)
        bottom_footer.addStretch()
        self.btn_clear_all = QPushButton("清空")
        # 宽度与“保存/登录”一致
        self.btn_clear_all.setFixedSize(60, 32)
        self.btn_clear_all.setCursor(Qt.PointingHandCursor)
        self.btn_clear_all.setStyleSheet(
            "QPushButton { background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 rgba(255, 200, 200, 0.75), stop:1 rgba(255, 150, 150, 0.60)); color: #2F2F2F; border: 2px solid #000000; border-radius: 12px; padding: 4px 10px; font-weight: bold; font-size: 12px; font-family: 'Comic Sans MS', 'Yuanti SC', 'STHeiti'; font-weight: bold; } QPushButton:hover:enabled { background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 rgba(255, 100, 100, 0.20), stop:1 rgba(255, 80, 80, 0.15)); margin-top: 1px; margin-left: 1px; } QPushButton:pressed:enabled { background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 rgba(255, 120, 120, 0.25), stop:1 rgba(255, 90, 90, 0.18)); } QPushButton:disabled { background: #ccc; color: #666; }"
        )
        self.btn_clear_all.clicked.connect(self.confirm_clear_all)
        bottom_footer.addWidget(self.btn_clear_all)
        account_mgmt_frame_layout.addLayout(bottom_footer)

        # 添加边框框到主布局，使用相同的居中布局，确保左右与输入框行对齐
        account_mgmt_wrapper = QHBoxLayout()
        account_mgmt_wrapper.setAlignment(
            Qt.AlignCenter
        )  # 居中对齐，与上面的输入框行一致
        account_mgmt_wrapper.setContentsMargins(0, 0, 0, 0)
        account_mgmt_wrapper.addStretch()  # 左侧弹性空间
        account_mgmt_wrapper.addWidget(account_mgmt_frame)
        account_mgmt_wrapper.addStretch()  # 右侧弹性空间

        # 添加到主布局，使用stretch factor让高度延伸到底部（距离底部10px）
        self.layout.addLayout(account_mgmt_wrapper, 1)  # 使用stretch factor让高度延伸
        self.layout.addSpacing(10)  # 底部边距10px

        # 初始化列表显示
        self.refresh_account_list()
    # 统一的弹窗样式函数，固定在GUI中央显示
    def show_message_box(self, icon, title, text, buttons=None):
        msg = QMessageBox(self)
        msg.setWindowFlags(Qt.FramelessWindowHint | Qt.Dialog)
        msg.setStyleSheet(
            f"""
            QMessageBox {{
                background-color: #FFF8E7;
                border: 3px solid #000000;
                border-radius: 18px;
                padding: 25px;
                min-width: 350px;
                max-width: 500px;
            }}
            QLabel {{
                color: #2F2F2F;
                font-size: 15px;
                font-weight: 600;
                padding: 15px;
                background: transparent;
            }}
            QPushButton {{
                border: 2px solid #000000;
                border-radius: 12px;
                padding: 10px 25px;
                background-color: #C8E6C9;
                color: #2F2F2F;
                font-size: 14px;
                font-weight: bold;
                min-width: 90px;
                font-family: 'Comic Sans MS', 'Yuanti SC', 'STHeiti'; font-weight: bold;
            }}
            QPushButton:hover {{
                background-color: #A5D6A7;
                border-width: 3px;
            }}
            QPushButton:pressed {{
                background-color: #81C784;
            }}
            QPushButton:default {{
                background-color: #4CAF50;
                color: white;
                border-width: 3px;
            }}
        """
        )
        msg.setIcon(icon)
        msg.setWindowTitle(title)
        msg.setText(text)
        if buttons:
            msg.setStandardButtons(buttons)

        msg.adjustSize()

        # 获取主窗口的几何信息，确保弹窗显示在主窗口中央
        main_window = None
        if hasattr(self, "main_window"):
            main_window = self.main_window
        else:
            main_window = self.window()

        if main_window:
            main_geometry = main_window.geometry()
        else:
            main_geometry = self.geometry()

        msg_geometry = msg.geometry()
        x = main_geometry.x() + (main_geometry.width() - msg_geometry.width()) // 2
        y = main_geometry.y() + (main_geometry.height() - msg_geometry.height()) // 2
        msg.move(x, y)

        return msg.exec_()
    # endregion

    # region  账号列表 +按钮 填充/删除/清空

    #创建表单
    def create_account_item(self, account, password, index, message_count, status="normal"):
        """创建单个账号项Widget"""
        item = QWidget()
        item.setFixedHeight(35)
        item.setCursor(Qt.PointingHandCursor)

        layout = QHBoxLayout(item)
        layout.setContentsMargins(4, 6, 6, 6)
        layout.setSpacing(4)

        # 根据状态设置颜色
        text_color = "#FF0000" if status == "fault" else "#2F2F2F"

        # 序号
        idx_lbl = QLabel(f"{index}.")
        idx_lbl.setFixedWidth(22)
        idx_lbl.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        idx_lbl.setStyleSheet(
            f"border:none;background:transparent;font-family: 'Comic Sans MS', 'Yuanti SC', 'STHeiti'; font-weight: bold; font-size:13px;color:{text_color};"
        )
        layout.addWidget(idx_lbl)

        # 账号（如果故障，添加标记）
        account_text = f"{account} / ****"
        if status == "fault":
            account_text += "  ⚠️ 账号故障"
        acc_lbl = QLabel(account_text)
        acc_lbl.setStyleSheet(
            f"border:none;background:transparent;font-family: 'Comic Sans MS', 'Yuanti SC', 'STHeiti'; font-weight: bold; font-size:13px;color:{text_color};font-weight:bold;"
        )
        layout.addWidget(acc_lbl, 1)

        # 删除按钮（末尾）
        del_btn = QPushButton("✖")
        del_btn.setFixedSize(20, 20)
        del_btn.setCursor(Qt.PointingHandCursor)
        del_btn.clicked.connect(lambda: self.delete_line(index - 1))
        del_btn.setStyleSheet(
            "QPushButton{border:none;background:transparent;color:rgba(255,0,0,0);}"
        )
        layout.addWidget(del_btn)

        # 悬停/按下效果
        def enter():
            del_btn.setStyleSheet(
                "QPushButton{border:none;background:transparent;color:#ff0000;font-weight:bold;}"
                "QPushButton:hover{background:rgba(255,200,200,0.3);border-radius:3px;}"
            )
            item.setStyleSheet(
                "QWidget{background:rgba(255,255,255,0.22);border:none;border-radius:10px;}"
            )

        def leave():
            del_btn.setStyleSheet(
                "QPushButton{border:none;background:transparent;color:rgba(255,0,0,0);}"
            )
            item.setStyleSheet("QWidget{background:transparent;border:none;}")

        def press():
            item.setStyleSheet(
                "QWidget{background:rgba(255,255,255,0.32);border:none;border-radius:10px;}"
            )

        item.enterEvent = lambda e: enter()
        item.leaveEvent = lambda e: leave()
        item.mousePressEvent = lambda e: press()
        item.mouseReleaseEvent = lambda e: enter() if item.underMouse() else leave()
        item.mouseDoubleClickEvent = lambda e: self.fill_account(account, password)

        return item
    #刷新列表
    def refresh_account_list(self):
        while self.account_list_layout.count():
            child = self.account_list_layout.takeAt(0)
            if child.widget():
                child.widget().deleteLater()

        for idx, line in enumerate(self.imported_lines, 1):
            # 兼容新旧格式
            if len(line) == 3:
                acc, pwd, status = line
            else:
                acc, pwd = line[0], line[1]
                status = "normal"
            item = self.create_account_item(acc, pwd, idx, 0, status)
            self.account_list_layout.addWidget(item)
    #填充
    def fill_account(self, account, password):
        """填充账号到输入框"""
        self.edit_id.setText(account)
        self.edit_pass.setText(password)
        self.last_used = account
        self.save_config()
    #删除
    def delete_line(self, index):
        """删除指定索引的账号（弹窗选择：本地删除 / 彻底删除）"""
        if 0 <= index < len(self.imported_lines):
            account = self.imported_lines[index]
            apple_id = account[0] if isinstance(account, (list, tuple)) else account

            # 创建自定义弹窗
            msg = QMessageBox(self)
            msg.setWindowFlags(Qt.FramelessWindowHint | Qt.Dialog)
            msg.setStyleSheet(f"""
                QMessageBox {{
                    background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #ffd9a0, stop:0.5 #ffbf70, stop:1 #ffa94d);
                border: 3px solid #000000;
                    border-radius: 15px;
                    padding: 20px;
                }}
                QLabel {{
                    color: #2F2F2F;
                    font-size: 14px;
                    font-weight: 600;
                    background: transparent;
                }}
                QPushButton {{
border: 2px solid #000000;
                    border-radius: 10px;
                    padding: 8px 20px;
                    font-size: 13px;
                    font-weight: bold;
                    min-width: 100px;
                    font-family: 'Comic Sans MS', 'Yuanti SC', 'STHeiti'; font-weight: bold;
                }}
                QPushButton:hover {{
                    border-width: 3px;
                }}
            """)
            msg.setIcon(QMessageBox.Question)
            msg.setWindowTitle("删除账号")
            msg.setText(f"删除账号: {apple_id}\n\n请选择删除方式：")

            # 添加自定义按钮
            btn_local = msg.addButton("删除本地", QMessageBox.AcceptRole)
            btn_local.setStyleSheet("""
                QPushButton {
                    background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #e3f2fd, stop:1 #bbdefb);
                    color: #1565C0;
                }
                QPushButton:hover {
                    background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #bbdefb, stop:1 #90caf9);
                }
            """)
            
            btn_full = msg.addButton("彻底删除", QMessageBox.DestructiveRole)
            btn_full.setStyleSheet("""
                QPushButton {
                    background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #ff5252, stop:1 #d32f2f);
                    color: white;
                }
                QPushButton:hover {
                    background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #ff1744, stop:1 #b71c1c);
                }
            """)
            
            btn_cancel = msg.addButton("取消", QMessageBox.RejectRole)
            btn_cancel.setStyleSheet("""
                QPushButton {
                    background: rgba(255, 255, 255, 0.5);
                    color: #2F2F2F;
                }
                QPushButton:hover {
                    background: rgba(255, 255, 255, 0.7);
                }
            """)

            msg.exec_()
            clicked = msg.clickedButton()

            if clicked == btn_local:
                # 只删除本地
                del self.imported_lines[index]
                self.refresh_account_list()
            elif clicked == btn_full:
                # 彻底删除（本地 + 数据库）
                self._run_async_in_thread(self._delete_account_from_db(apple_id))
                del self.imported_lines[index]
                self.refresh_account_list()
    #清空
    def confirm_clear_all(self):
        """清空所有账号（弹窗选择：本地清空 / 彻底清空）"""
        if not self.imported_lines:
            return

        msg = QMessageBox(self)
        msg.setWindowFlags(Qt.FramelessWindowHint | Qt.Dialog)
        msg.setStyleSheet(f"""
            QMessageBox {{
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #ffd9a0, stop:0.5 #ffbf70, stop:1 #ffa94d);
                border: 3px solid #000000;
                border-radius: 15px;
                padding: 20px;
            }}
            QLabel {{
                color: #2F2F2F;
                font-size: 14px;
                font-weight: 600;
                background: transparent;
            }}
            QPushButton {{
                border: 2px solid #000000;
                border-radius: 10px;
                padding: 8px 20px;
                font-size: 13px;
                font-weight: bold;
                min-width: 100px;
                font-family: 'Comic Sans MS', 'Yuanti SC', 'STHeiti'; font-weight: bold;
            }}
            QPushButton:hover {{
                border-width: 3px;
            }}
        """)
        msg.setIcon(QMessageBox.Warning)
        msg.setWindowTitle("清空账号列表")
        msg.setText(f"共 {len(self.imported_lines)} 个账号\n\n请选择清空方式：")

        btn_local = msg.addButton("清空本地", QMessageBox.AcceptRole)
        btn_local.setStyleSheet("""
            QPushButton {
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #e3f2fd, stop:1 #bbdefb);
                color: #1565C0;
            }
            QPushButton:hover {
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #bbdefb, stop:1 #90caf9);
            }
        """)
        
        btn_full = msg.addButton("彻底清空", QMessageBox.DestructiveRole)
        btn_full.setStyleSheet("""
            QPushButton {
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #ff5252, stop:1 #d32f2f);
                color: white;
            }
            QPushButton:hover {
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #ff1744, stop:1 #b71c1c);
            }
        """)
        
        btn_cancel = msg.addButton("取消", QMessageBox.RejectRole)
        btn_cancel.setStyleSheet("""
            QPushButton {
                background: rgba(255, 255, 255, 0.5);
                color: #2F2F2F;
            }
            QPushButton:hover {
                background: rgba(255, 255, 255, 0.7);
            }
        """)

        msg.exec_()
        clicked = msg.clickedButton()

        if clicked == btn_local:
            # 只清空本地
            self.imported_lines = []
            self.refresh_account_list()
        elif clicked == btn_full:
            # 彻底清空（本地 + 数据库）
            async def delete_all():
                for account in self.imported_lines:
                    apple_id = account[0] if isinstance(account, (list, tuple)) else account
                    await self._delete_account_from_db(apple_id)

            self._run_async_in_thread(delete_all())
            self.imported_lines = []
            self.refresh_account_list()
    # endregion

    # region  按钮 保存/登录/导入

    # 加载账号列表到GUI（数据库 + 本地缓存）
    def load_config(self):
        """从数据库加载账号列表"""
        self.accounts = []
        self.passwords = {}
        self.imported_lines = (
            []
        )  # 格式: [(account, password, status), ...] status: "normal" 或 "fault"

        # 异步从数据库加载
        def load_from_db():
            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                accounts = loop.run_until_complete(self._fetch_accounts_from_db())
                loop.close()

                # 更新到主线程
                self.imported_lines = accounts
                for acc, pwd, status in accounts:
                    if acc not in self.accounts:
                        self.accounts.append(acc)
                    self.passwords[acc] = pwd

                # 刷新UI（需要在主线程执行）
                QTimer.singleShot(0, self.refresh_account_list)
            except Exception as e:
                log(f"[{now_iso()}][WORKER][ERRO][6079][load_from_db][加载账号列表失败: {e}]")
                # 如果数据库加载失败，尝试从 JSON 文件加载（兼容性）
                if os.path.exists(self.config_file):
                    try:
                        with open(self.config_file, "r", encoding="utf-8") as f:
                            data = json.load(f)
                            self.accounts = data.get("accounts", [])[:5]
                            self.passwords = data.get("passwords", {})
                            self.last_used = data.get("last_used")
                            imported = data.get("imported_lines", [])
                            self.imported_lines = []
                            for item in imported:
                                if isinstance(item, (list, tuple)):
                                    if len(item) == 2:
                                        self.imported_lines.append(
                                            (item[0], item[1], "normal")
                                        )
                                    elif len(item) >= 3:
                                        self.imported_lines.append(
                                            (item[0], item[1], item[2])
                                        )
                    except:
                        pass

        # 在后台线程加载
        thread = threading.Thread(target=load_from_db, daemon=True)
        thread.start()

    # 保存配置（本地 + 数据库）
    def save_config(self):
        """保存账号列表（数据库 + 本地缓存）"""
        if not self.imported_lines:
            return

        # 保存到本地 JSON 缓存
        self._save_to_local_json()
        
        # 如果有 API，也保存到数据库
        if self.get_api_base_url():
            self._run_async_in_thread(self._save_accounts_to_db(self.imported_lines))
  
    # 导入
    def import_accounts_file(self):
        fname, _ = QFileDialog.getOpenFileName(
            self, "选择账号文件", "", "文本文件 (*.txt);;所有文件 (*)"
        )
        if not fname:
            return
        try:
            with open(fname, "r", encoding="utf-8") as f:
                lines = [line.strip() for line in f.readlines() if line.strip()]
            new_accounts = []
            for line in lines:
                parts = [p.strip() for p in line.replace(",", " ").split() if p.strip()]
                if len(parts) >= 2:
                    acc, pwd = parts[0], parts[1]
                    new_accounts.append((acc, pwd, "normal"))

            # 合并到现有列表（去重）
            existing_accounts = {acc[0].lower() for acc in self.imported_lines}
            for acc, pwd, status in new_accounts:
                if acc.lower() not in existing_accounts:
                    self.imported_lines.append((acc, pwd, status))
                    existing_accounts.add(acc.lower())

            # 保存到数据库
            self.save_config()
            self.refresh_account_list()  # 刷新账号列表显示（表格形式）
        except Exception as e:
            msg = QMessageBox(self)
            msg.setWindowFlags(Qt.FramelessWindowHint | Qt.Dialog)
            msg.setStyleSheet(
                "QMessageBox { background-color: #FFF8E7; border: 2px solid #2F2F2F; border-radius: 10px; }"
                "QLabel { color: #2F2F2F; font-size: 13px; }"
                "QPushButton { border: 2px solid #2F2F2F; border-radius: 8px; padding: 5px 15px; background: #C8E6C9; }"
                "QPushButton:hover { margin-top: 2px; margin-left: 2px; }"
            )
            msg.setIcon(QMessageBox.Critical)
            msg.setWindowTitle("错误")
            msg.setText(f"导入失败: {str(e)}")
            msg.exec_()
   
    # 保存当前编辑框中的账号密码，并更新账号列表
    def save_current_account(self):
        account = self.edit_id.text().strip()
        password = self.edit_pass.text().strip()
        if not account or not password:
            msg = QMessageBox(self)
            msg.setWindowFlags(Qt.FramelessWindowHint | Qt.Dialog)
            msg.setStyleSheet(
                "QMessageBox { background-color: #FFF8E7; border: 2px solid #2F2F2F; border-radius: 10px; }"
                "QLabel { color: #2F2F2F; font-size: 13px; }"
                "QPushButton { border: 2px solid #2F2F2F; border-radius: 8px; padding: 5px 15px; background: #C8E6C9; }"
                "QPushButton:hover { margin-top: 2px; margin-left: 2px; }"
            )
            msg.setIcon(QMessageBox.Warning)
            msg.setWindowTitle("提示")
            msg.setText("账号和密码不能为空")
            msg.exec_()
            return

        # 去重并保存（保留状态）
        self.imported_lines = [
            (a, p, s) if len(item) == 3 else (a, p, "normal")
            for item in self.imported_lines
            for a, p, *rest in [item if len(item) == 3 else (*item, "normal")]
            for s in [rest[0] if rest else "normal"]
            if a != account
        ]
        self.imported_lines.insert(0, (account, password, "normal"))

        # 保存到数据库
        self.save_config()
        self.refresh_account_list()  # 刷新账号列表显示（表格形式）
 
    # 显示密码
    def toggle_password_visibility(self):
        """切换密码显示/隐藏"""
        if self.edit_pass.echoMode() == QLineEdit.Password:
            self.edit_pass.setEchoMode(QLineEdit.Normal)
            self.btn_toggle_pass.setText("🙈")
        else:
            self.edit_pass.setEchoMode(QLineEdit.Password)
            self.btn_toggle_pass.setText("👁")
 
    # 登录
    def accept_login(self):
        account = self.edit_id.text().strip()
        password = self.edit_pass.text()
        if not account or not password:
            msg = QMessageBox(self)
            msg.setWindowFlags(Qt.FramelessWindowHint | Qt.Dialog)
            msg.setStyleSheet(
                "QMessageBox { background-color: #FFF8E7; border: 2px solid #2F2F2F; border-radius: 10px; }"
                "QLabel { color: #2F2F2F; font-size: 13px; }"
                "QPushButton { border: 2px solid #2F2F2F; border-radius: 8px; padding: 5px 15px; background: #C8E6C9; }"
                "QPushButton:hover { margin-top: 2px; margin-left: 2px; }"
            )
            msg.setIcon(QMessageBox.Warning)
            msg.setWindowTitle("提示")
            msg.setText("Apple ID 和密码不能为空")
            msg.exec_()
            return

        self.last_used = account
        self.passwords[account] = password
        self.save_config()

        # 执行登录
        login_success = self.run_login_script(account, password)

    # endregion

    # region  登录脚本MESSAGE

    def run_login_script(self, account_id, password, timeout=15):

        # 检查辅助功能权限
        check_cmd = "osascript -e 'tell application \"System Events\" to get name of processes' 2>/dev/null"
        has_permission = subprocess.call(check_cmd, shell=True) == 0

        # 弹窗
        if not has_permission:
            subprocess.Popen(
                [
                    "osascript",
                    "-e",
                    'button returned of (display dialog "需要添加终端辅助权限才能自动登录\\n\\n点击「打开设置」后:\\n1. 点击🔒解锁\\n2. 勾选✅Terminal\\n3. 关闭窗口" buttons {"稍后添加", "打开设置"} default button 2 with icon caution)',
                    "-e",
                    'if result is "打开设置" then do shell script "open \\"x-apple.systempreferences:com.apple.preference.security?Privacy_Accessibility\\""',
                ]
            )

        applescript = f"""
        on run argv
            if (count of argv) < 2 then
                return "error:missing_args"
            end if
            set account to item 1 of argv
            set pwd to item 2 of argv

            -- 先激活 Messages,确保程序前置
            tell application "Messages" to activate

            tell application "System Events"
                set t0 to (current date)
                -- 重复等待，直到找到 window 1 且 window 有 text field(输入框)
                repeat
                    try
                        if (exists process "Messages") then
                            if (exists window 1 of process "Messages") then
                                -- 如果窗口中有 text field(登录输入框)，退出循环
                                if (exists text field 1 of window 1 of process "Messages") then
                                    exit repeat
                                end if
                            end if
                        end if
                    end try
                    delay 0.5
                    if ((current date) - t0) > {timeout} then
                        return "timeout"
                    end if
                end repeat

                -- 确保前端focus稳定
                delay 0.2

                -- 输入账号并回车，等一会再输入密码并回车
                keystroke account
                delay 0.2
                key code 36 -- return
                delay 0.5
                keystroke pwd
                delay 0.2
                key code 36 -- return
            end tell

            -- 等待5秒，然后检查登录是否成功
            delay 5
            
            tell application "System Events"
                try
                    -- 如果还存在登录输入框，说明登录失败
                    if (exists text field 1 of window 1 of process "Messages") then
                        return "login_failed"
                    end if
                end try
            end tell
            
            -- 登录窗口消失，说明登录成功
            return "ok"
        end run
        """

        try:
            # 把脚本写成临时文件并执行（避免命令行转义问题）
            tmp = os.path.join(self.config_dir, "tmp_autologin.scpt")
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(applescript)

            # 运行 osascript，传入账号和密码作为 argv
            process = subprocess.Popen(
                ["osascript", tmp, account_id, password],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            stdout, stderr = process.communicate(timeout=timeout + 10)

            # 清理临时脚本
            try:
                os.remove(tmp)
            except:
                pass

            out = stdout.decode("utf-8", errors="ignore").strip()
            err = stderr.decode("utf-8", errors="ignore").strip()
            if process.returncode != 0:
                return False

            if out == "ok":
                return True
            elif out == "login_failed":
                return False  # 登录窗口依然存在，登录失败
            elif out == "timeout":
                return False
            else:
                return False

        except subprocess.TimeoutExpired:
            return False
        except Exception as e:
            return False

    def submit_2fa_code(self, code, timeout=10):
        """通过 AppleScript 将验证码输入到弹窗"""
        if not code:
            return False

        applescript = """
        on run argv
            if (count of argv) < 1 then
                return "error:missing_code"
            end if
            set otp to item 1 of argv

            tell application "System Events"
                keystroke otp
                delay 0.2
                key code 36
            end tell

            return "ok"
        end run
        """

        tmp = os.path.join(self.config_dir, "tmp_submit_code.scpt")
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(applescript)

            process = subprocess.Popen(
                ["osascript", tmp, code],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            stdout, stderr = process.communicate(timeout=timeout + 5)
            out = stdout.decode("utf-8", errors="ignore").strip()
            if process.returncode != 0 or out != "ok":
                return False
            return True
        except Exception:
            return False
        finally:
            try:
                os.remove(tmp)
            except:
                pass

    # endregion

    # region  数据库操作

    # 获取API基础URL，优先从当前连接获取，失败则从配置文件读取
    def get_api_base_url(self):
        """获取 API 基础 URL（优化版）"""
        # 1. 尝试从 server 对象获取
        try:
            if hasattr(self.main_window, "panel_backend"):
                backend = self.main_window.panel_backend
                if backend and hasattr(backend, "server") and backend.server:
                    api_url = backend.server.api_base_url
                    if api_url:
                        return api_url.rstrip("/")
        except:
            pass
        
        # 2. 尝试从统一配置文件获取（server_config.json）
        try:
            if hasattr(self.main_window, "panel_backend"):
                backend = self.main_window.panel_backend
                if backend and hasattr(backend, "load_backend_config"):
                    cfg = backend.load_backend_config() or {}
                    saved_url = (cfg.get("api_url") or "").strip()
                    if saved_url:
                        if not saved_url.startswith(("http://", "https://")):
                            # 本地地址默认走 http，避免 https/ssl 连接本地 http 端口导致 WRONG_VERSION_NUMBER
                            s = saved_url.lower()
                            if s.startswith("localhost") or s.startswith("127.0.0.1") or s.startswith("0.0.0.0"):
                                saved_url = "http://" + saved_url
                            else:
                                saved_url = "https://" + saved_url
                        return saved_url.rstrip("/")
        except:
            pass
        
        # 3. 尝试从环境变量获取
        api_url = os.getenv("API_BASE_URL", "").strip()
        if api_url:
            return api_url.rstrip("/")
        
        return None
    # 从数据库获取账号列表
    async def _fetch_accounts_from_db(self):
        """从数据库获取账号列表"""
        api_url = self.get_api_base_url()
        if not api_url:
            return []

        try:
            import ssl
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
            connector = aiohttp.TCPConnector(ssl=ssl_context)
            
            async with aiohttp.ClientSession(connector=connector) as session:
                async with session.get(
                    f"{api_url}/id-library", timeout=aiohttp.ClientTimeout(total=10)
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data.get("success") and data.get("accounts"):
                            accounts = []
                            for acc in data["accounts"]:
                                accounts.append(
                                    (
                                        acc.get("appleId", ""),
                                        acc.get("password", ""),
                                        acc.get("status", "normal"),
                                    )
                                )
                            return accounts
        except Exception as e:
            log(f"[{now_iso()}][WORKER][ERRO][6424][_get_accounts_from_db][从数据库获取账号列表失败: {e}]")
        return []

    # 保存账号列表到数据库
    async def _save_accounts_to_db(self, accounts):
        """保存账号列表到数据库"""
        api_url = self.get_api_base_url()
        if not api_url:
            return False

        try:
            accounts_data = []
            for acc, pwd, status in accounts:
                accounts_data.append(
                    {
                        "appleId": acc,
                        "password": pwd,
                        "status": status,
                        "usageStatus": "new",
                    }
                )

            import ssl
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
            connector = aiohttp.TCPConnector(ssl=ssl_context)
            
            async with aiohttp.ClientSession(connector=connector) as session:
                async with session.post(
                    f"{api_url}/id-library",
                    json={"accounts": accounts_data},
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as response:
                    if response.status == 200:
                        result = await response.json()
                        return result.get("success", False)
        except Exception as e:
            log(f"[{now_iso()}][WORKER][ERRO][6462][_save_accounts_to_db][保存账号列表到数据库失败: {e}]")
        return False

    # 从数据库删除指定账号
    async def _delete_account_from_db(self, apple_id):
        """从数据库删除指定账号"""
        api_url = self.get_api_base_url()
        if not api_url:
            return False

        try:
            import ssl
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
            connector = aiohttp.TCPConnector(ssl=ssl_context)
            
            async with aiohttp.ClientSession(connector=connector) as session:
                async with session.delete(
                    f"{api_url}/id-library/{apple_id}",
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as response:
                    if response.status == 200:
                        result = await response.json()
                        return result.get("success", False)
        except Exception as e:
            log(f"[{now_iso()}][WORKER][ERRO][6488][_delete_account_from_db][从数据库删除账号失败: {e}]")
        return False

    # 获取SSL连接器，用于API请求
    def _get_ssl_connector(self):
        try:
            if (
                hasattr(self.main_window, "panel_backend")
                and self.main_window.panel_backend.server
            ):
                return self.main_window.panel_backend.server._get_ssl_connector()
        except:
            pass
        import ssl

        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        return aiohttp.TCPConnector(ssl=ssl_context)

    # 在线程中安全运行异步函数
    def _run_async_in_thread(self, coro):
        def run_in_thread():
            try:
                if (
                    hasattr(self.main_window, "panel_backend")
                    and self.main_window.panel_backend.server
                ):
                    server = self.main_window.panel_backend.server
                    if hasattr(server, "loop") and server.loop:
                        asyncio.run_coroutine_threadsafe(coro, server.loop)
                        return
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                loop.run_until_complete(coro)
                loop.close()
            except Exception as e:
                log(f"[{now_iso()}][WORKER][ERRO][6525][_run_async][运行异步函数失败: {e}]")

        thread = threading.Thread(target=run_in_thread, daemon=True)
        thread.start()

    def _save_to_local_json(self):
        try:
            data = {"imported_lines": list(self.imported_lines)}
            with open(self.config_file, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            log(f"[{now_iso()}][WORKER][ERRO][6537][_save_to_local_json][保存本地缓存失败: {e}]")

    # endregion

class PanelTools(FixedSizePanel):

    # region  init 
    def __init__(self, parent_window):
        gradient_bg = "qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #F48FB1, stop:0.5 #F06292, stop:1 #EC407A)"
        super().__init__(gradient_bg, 550, 430, parent_window)
        self.main_window = parent_window
        self.datapath = get_app_data_dir("Autosender Pro")
        self.last_system_diag_report = None
        self.last_db_diag_report = None
        self.reports_dir = self.datapath
        os.makedirs(self.reports_dir, exist_ok=True)
        self._call_source = "local"
        self._setup_ui()

    def _setup_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        # 标题栏 - 移除渐变背景
        self.header = QFrame()
        self.header.setFixedHeight(35)
        self.header.setStyleSheet("QFrame { background: transparent; border: none; border-bottom: 2px solid #000000; border-top-left-radius: 18px; border-top-right-radius: 18px; border-bottom-left-radius: 0px; border-bottom-right-radius: 0px; }")
        header_layout = QHBoxLayout(self.header)
        header_layout.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        header_layout.setContentsMargins(13, 0, 0, 0)
        header_layout.setSpacing(0)
        lbl_title = QLabel("修复工具")
        lbl_title.setStyleSheet(
            "border: none; color: #2F2F2F; font-family: 'Comic Sans MS', 'Yuanti SC', 'STHeiti'; font-weight: bold; font-size: 15px; padding: 0px;"
        )
        header_layout.addWidget(lbl_title)
        header_layout.addStretch()
        layout.addWidget(self.header)

        # 功能按钮区域 - 去掉边框，只保留面板外边框
        function_panel = QFrame()
        function_panel.setStyleSheet("background: transparent; border: none;")
        function_layout = QVBoxLayout(function_panel)
        function_layout.setContentsMargins(15, 15, 15, 15)
        function_layout.setSpacing(12)

        # 1. 系统检测 - 蓝色渐变
        sys_check_row = QHBoxLayout()
        sys_check_row.setSpacing(15)
        sys_check_row.addSpacing(30)  # 按钮右移50
        self.btn_system_check = QPushButton("系统检测")
        self.btn_system_check.setFixedSize(100, 35)  # 宽度减少20
        self.btn_system_check.setStyleSheet(
            f"""
            QPushButton {{
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #4FC3F7, stop:1 #29B6F6);
                color: white;
                border: 2px solid #000000;
                border-radius: 12px;
                font-family: 'Comic Sans MS', 'Yuanti SC', 'STHeiti'; font-weight: bold; font-size: 13px;
                padding: 5px 15px;
            }}
            QPushButton:hover {{
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #29B6F6, stop:1 #0288D1);
                margin-top: 2px;
                margin-left: 2px;
            }}
            QPushButton:pressed {{
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #0288D1, stop:1 #0277BD);
                margin-top: 3px;
                margin-left: 3px;
            }}
        """
        )
        self.btn_system_check.clicked.connect(lambda: self._safe_invoke(self.run_diagnose, source="local"))
        # 添加右键菜单功能
        self.btn_system_check.setContextMenuPolicy(Qt.CustomContextMenu)
        self.btn_system_check.customContextMenuRequested.connect(
            lambda: self._open_last_report("system")
        )
        sys_check_row.addWidget(self.btn_system_check)
        sys_label = QLabel("检查系统环境和依赖 | 安全检测，不修改系统")
        sys_label.setStyleSheet(
            f"color: #2F2F2F; font-family: 'Comic Sans MS', 'Yuanti SC', 'STHeiti'; font-weight: bold; font-size: 12px; padding: 5px 0px;"
        )
        sys_check_row.addWidget(sys_label)
        sys_check_row.addStretch()
        function_layout.addLayout(sys_check_row)

        # 2. 数据库检测 - 绿色渐变
        db_check_row = QHBoxLayout()
        db_check_row.setSpacing(15)
        db_check_row.addSpacing(30)  # 按钮右移50
        self.btn_database_check = QPushButton("数据库检测")
        self.btn_database_check.setFixedSize(100, 35)  # 宽度减少20
        self.btn_database_check.setStyleSheet(
            f"""
            QPushButton {{
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #66BB6A, stop:1 #4CAF50);
                color: white;
                border: 2px solid #000000;
                border-radius: 12px;
                font-family: 'Comic Sans MS', 'Yuanti SC', 'STHeiti'; font-weight: bold; font-size: 13px;
                padding: 5px 15px;
            }}
            QPushButton:hover {{
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #4CAF50, stop:1 #388E3C);
                margin-top: 2px;
                margin-left: 2px;
            }}
            QPushButton:pressed {{
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #388E3C, stop:1 #2E7D32);
                margin-top: 3px;
                margin-left: 3px;
            }}
        """
        )
        self.btn_database_check.clicked.connect(lambda: self._safe_invoke(self.run_database_diagnose, source="local"))
        # 添加右键菜单功能
        self.btn_database_check.setContextMenuPolicy(Qt.CustomContextMenu)
        self.btn_database_check.customContextMenuRequested.connect(
            lambda: self._open_last_report("database")
        )
        db_check_row.addWidget(self.btn_database_check)
        db_label = QLabel("检查数据库完整性和连接 | 安全检测，不修改数据")
        db_label.setStyleSheet(
            f"color: #2F2F2F; font-family: 'Comic Sans MS', 'Yuanti SC', 'STHeiti'; font-weight: bold; font-size: 12px; padding: 5px 0px;"
        )
        db_check_row.addWidget(db_label)
        db_check_row.addStretch()
        function_layout.addLayout(db_check_row)

        # 3. 权限修复 - 橙色渐变
        perm_fix_row = QHBoxLayout()
        perm_fix_row.setSpacing(15)
        perm_fix_row.addSpacing(30)  # 按钮右移50
        self.btn_permission_fix = QPushButton("权限修复")
        self.btn_permission_fix.setFixedSize(100, 35)  # 宽度减少20
        self.btn_permission_fix.setStyleSheet(
            f"""
            QPushButton {{
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #FFA726, stop:1 #FF9800);
                color: white;
                border: 2px solid #000000;
                border-radius: 12px;
                font-family: 'Comic Sans MS', 'Yuanti SC', 'STHeiti'; font-weight: bold; font-size: 13px;
                padding: 5px 15px;
            }}
            QPushButton:hover {{
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #FF9800, stop:1 #F57C00);
                margin-top: 2px;
                margin-left: 2px;
            }}
            QPushButton:pressed {{
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #F57C00, stop:1 #E65100);
                margin-top: 3px;
                margin-left: 3px;
            }}
        """
        )
        self.btn_permission_fix.clicked.connect(lambda: self._safe_invoke(self.run_permission_fix, source="local"))
        perm_fix_row.addWidget(self.btn_permission_fix)
        perm_label = QLabel("修复文件和访问权限 | 修改系统权限配置")
        perm_label.setStyleSheet(
            f"color: #2F2F2F; font-family: 'Comic Sans MS', 'Yuanti SC', 'STHeiti'; font-weight: bold; font-size: 12px; padding: 5px 0px;"
        )
        perm_fix_row.addWidget(perm_label)
        perm_fix_row.addStretch()
        function_layout.addLayout(perm_fix_row)

        # 4. 清空收件箱 - 红色渐变
        clear_inbox_row = QHBoxLayout()
        clear_inbox_row.setSpacing(15)
        clear_inbox_row.addSpacing(30)  # 按钮右移50
        self.btn_clear_inbox = QPushButton("清空收件箱")
        self.btn_clear_inbox.setFixedSize(100, 35)  # 宽度减少20
        self.btn_clear_inbox.setStyleSheet(
            f"""
            QPushButton {{
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #EF5350, stop:1 #F44336);
                color: white;
                border: 2px solid #000000;
                border-radius: 12px;
                font-family: 'Comic Sans MS', 'Yuanti SC', 'STHeiti'; font-weight: bold; font-size: 13px;
                padding: 5px 15px;
            }}
            QPushButton:hover {{
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #F44336, stop:1 #D32F2F);
                margin-top: 2px;
                margin-left: 2px;
            }}
            QPushButton:pressed {{
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #D32F2F, stop:1 #C62828);
                margin-top: 3px;
                margin-left: 3px;
            }}
        """
        )
        self.btn_clear_inbox.clicked.connect(lambda: self._safe_invoke(self.clear_imessage_inbox, source="local"))
        clear_inbox_row.addWidget(self.btn_clear_inbox)
        clear_label = QLabel("清空iMessage收件箱 | 永久删除所有聊天记录")
        clear_label.setStyleSheet(
            f"color: #2F2F2F; font-family: 'Comic Sans MS', 'Yuanti SC', 'STHeiti'; font-weight: bold; font-size: 12px; padding: 5px 0px;"
        )
        clear_inbox_row.addWidget(clear_label)
        clear_inbox_row.addStretch()
        function_layout.addLayout(clear_inbox_row)

        # 5. 超级修复 - 紫色渐变
        super_fix_row = QHBoxLayout()
        super_fix_row.setSpacing(15)
        super_fix_row.addSpacing(30)  # 按钮右移50
        self.btn_super_fix = QPushButton("超级修复")
        self.btn_super_fix.setFixedSize(100, 35)  # 宽度减少20
        self.btn_super_fix.setStyleSheet(
            f"""
            QPushButton {{
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #BA68C8, stop:1 #AB47BC);
                color: white;
                border: 2px solid #000000;
                border-radius: 12px;
                font-family: 'Comic Sans MS', 'Yuanti SC', 'STHeiti'; font-weight: bold; font-size: 13px;
                padding: 5px 15px;
            }}
            QPushButton:hover {{
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #AB47BC, stop:1 #8E24AA);
                margin-top: 2px;
                margin-left: 2px;
            }}
            QPushButton:pressed {{
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #8E24AA, stop:1 #6A1B9A);
                margin-top: 3px;
                margin-left: 3px;
            }}
        """
        )
        self.btn_super_fix.clicked.connect(lambda: self._safe_invoke(self.run_hard_reset, source="local"))
        super_fix_row.addWidget(self.btn_super_fix)
        super_label = QLabel("执行全面深度修复 | 删除所有数据并可能重启系统")
        super_label.setStyleSheet(
            f"color: #2F2F2F; font-family: 'Comic Sans MS', 'Yuanti SC', 'STHeiti'; font-weight: bold; font-size: 12px; padding: 5px 0px;"
        )
        super_fix_row.addWidget(super_label)
        super_fix_row.addStretch()
        function_layout.addLayout(super_fix_row)

        # 添加到主布局
        layout.addWidget(function_panel)

    # endregion
    
    # region  辅助函数

    # try/except防崩溃
    def _safe_invoke(self, fn, *args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            try:
                log("ERRO", 0, "PanelTools._safe_invoke", f"工具执行崩溃: {e}")
            except Exception:
                pass
            try:
                if getattr(self, "_call_source", "local") == "local":
                    self.show_message_box(QMessageBox.Critical, "错误", f"工具执行失败: {str(e)}")
            except Exception:
                pass
            try:
                backend = getattr(self.main_window, "panel_backend", None)
                if backend and hasattr(backend, "log_message"):
                    backend.log_message(f"❌ 工具执行失败: {str(e)}")
            except Exception:
                pass
            return None

    # 远程报告
    def _remote_log(self, message: str):
        try:
            backend = getattr(self.main_window, "panel_backend", None)
            if backend and hasattr(backend, "log_message"):
                backend.log_message(message)
        except Exception:
            pass

    # endregion

    # region 弹窗位置

    def _center_message_box(self, msg):
        msg.adjustSize()

        # 获取主窗口的几何信息，确保弹窗显示在主窗口中央
        main_window = None
        if hasattr(self, "main_window"):
            main_window = self.main_window
        else:
            main_window = self.window()

        if main_window:
            main_geometry = main_window.geometry()
        else:
            main_geometry = self.geometry()

        # 获取屏幕尺寸
        screen = QApplication.primaryScreen().geometry()
        screen_width = screen.width()
        screen_height = screen.height()

        msg_geometry = msg.geometry()
        msg_width = msg_geometry.width()
        msg_height = msg_geometry.height()

        # 计算居中位置
        x = main_geometry.x() + (main_geometry.width() - msg_width) // 2
        y = main_geometry.y() + (main_geometry.height() - msg_height) // 2

        # 确保不超出屏幕范围
        if x < screen.x():
            x = screen.x() + 20  # 左边距20px
        elif x + msg_width > screen.x() + screen_width:
            x = screen.x() + screen_width - msg_width - 20  # 右边距20px

        if y < screen.y():
            y = screen.y() + 20  # 上边距20px
        elif y + msg_height > screen.y() + screen_height:
            y = screen.y() + screen_height - msg_height - 20  # 下边距20px

        msg.move(x, y)

    def show_message_box(self, icon, title, text, buttons=None):
        """统一的弹窗样式函数，固定在GUI中央显示"""
        msg = QMessageBox(self)
        msg.setWindowFlags(Qt.FramelessWindowHint | Qt.Dialog)

        # 限制文本长度，避免弹窗过大
        max_text_length = 800  # 大约限制在800字符以内
        if len(text) > max_text_length:
            text = (
                text[:max_text_length]
                + "\n\n... (内容过长，已截断，请查看完整报告文件)"
            )

        msg.setStyleSheet(
            f"""
            QMessageBox {{
                background-color: #FFF8E7;
                border: 3px solid #000000;
                border-radius: 18px;
                padding: 20px;
                min-width: 350px;
                max-width: 600px;
            }}
            QLabel {{
                color: #2F2F2F;
                font-size: 13px;
                font-weight: 600;
                padding: 10px;
                background: transparent;
            }}
            QPushButton {{
                border: 2px solid #000000;
                border-radius: 12px;
                padding: 10px 25px;
                background-color: #C8E6C9;
                color: #2F2F2F;
                font-size: 14px;
                font-weight: bold;
                min-width: 90px;
                font-family: 'Comic Sans MS', 'Yuanti SC', 'STHeiti'; font-weight: bold;
            }}
            QPushButton:hover {{
                background-color: #A5D6A7;
                border-width: 3px;
            }}
            QPushButton:pressed {{
                background-color: #81C784;
            }}
            QPushButton:default {{
                background-color: #4CAF50;
                color: white;
                border-width: 3px;
            }}
        """
        )
        msg.setIcon(icon)
        msg.setWindowTitle(title)
        msg.setText(text)
        if buttons:
            msg.setStandardButtons(buttons)

        # 使用全局定位函数
        self._center_message_box(msg)

        return msg.exec_()

    # endregion

    # region 右键打开上一次报告

    def _open_last_report(self, report_type):
        """右键点击按钮时打开上一次的报告文件"""
        if report_type == "system":
            last_report = self.last_system_diag_report
            report_name = "系统检测"
        elif report_type == "database":
            last_report = self.last_db_diag_report
            report_name = "数据库检测"
        else:
            return

        if last_report and os.path.exists(last_report):
            try:
                subprocess.run(["open", last_report])
            except Exception as e:
                self.show_message_box(
                    QMessageBox.Warning, "提示", f"无法打开报告文件: {str(e)}"
                )

    # endregion

    # region 运行命令函数

    def run_with_auth(self, cmd: str):
        safe = cmd.replace("\\", "\\\\").replace('"', '\\"')
        applescript = f"""do shell script "{safe}" with administrator privileges"""
        p = subprocess.Popen(
            ["osascript", "-e", applescript],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        out, err = p.communicate()
        return p.returncode, out.strip(), err.strip()

    def run_cmd_local(self, cmd: str):
        p = subprocess.Popen(
            cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
        out, err = p.communicate()
        return p.returncode, out.strip(), err.strip()

    # endregion

    # region 修复权限（使用线程）

    def _run_permission_fix_thread(self):
        try:
            get_permissions_cmd = """
chmod -R 755 /Library/Preferences/com.apple.apsd.plist /Library/Preferences/com.apple.ids.service* /Library/Preferences/com.apple.imfoundation* 2>/dev/null || true
chmod -R 755 ~/Library/Preferences/com.apple.iChat* ~/Library/Preferences/com.apple.immessage* ~/Library/Preferences/com.apple.ids.service* ~/Library/Preferences/com.apple.identityservices* ~/Library/Preferences/com.apple.imfoundation* 2>/dev/null || true
chmod -R 755 ~/Library/Caches/com.apple.Messages ~/Library/Caches/com.apple.apsd ~/Library/Caches/com.apple.imfoundation* ~/Library/Caches/com.apple.identityservices* 2>/dev/null || true
chown -R "$USER" ~/Library/Preferences/com.apple.* ~/Library/Caches/com.apple.* 2>/dev/null || true
/usr/bin/killall -HUP mDNSResponder 2>/dev/null || true
/usr/bin/killall -9 apsd 2>/dev/null || true
"""
            ret, out, err = self.run_with_auth(get_permissions_cmd)
            result_msg = (
                "✅ 权限修复完成"
                if ret == 0
                else f"⚠️ 权限修复结束，退出码 {ret}"
            )
            log("INFO", 6976, "_run_permission_fix_thread", result_msg)
            if self._call_source == "local":
                QTimer.singleShot(
                    0,
                    lambda: self.show_message_box(
                        QMessageBox.Information, "完成", result_msg
                    ),
                )
        except Exception as e:
            error_msg = f"权限修复出错: {str(e)}"
            log("ERRO", 6991, "_run_permission_fix_thread", error_msg)
            if self._call_source == "local":
                QTimer.singleShot(
                    0,
                    lambda: self.show_message_box(QMessageBox.Critical, "错误", error_msg),
                )

    def run_permission_fix(self, source="local"):
        self._call_source = source
        if source == "remote":
            self._remote_log("开始执行: 权限修复")
        else:
            try:
                if hasattr(self.main_window, "system_log"):
                    self.main_window.system_log("开始执行: 权限修复")
            except Exception:
                pass
            try:
                log("INFO", 7014, "run_permission_fix", "开始执行: 权限修复")
            except Exception:
                pass
        if sys.platform != "darwin":
            if source == "local":
                self.show_message_box(
                    QMessageBox.Warning, "提示", "此功能仅在 macOS 系统上可用"
                )
            else:
                log("ERRO", 7002, "run_permission_fix", "此功能仅在 macOS 系统上可用")
                self._remote_log("⚠️ 权限修复仅支持 macOS")
            return
        if source == "local":
            reply = self.show_message_box(
                QMessageBox.Question,
                "确认",
                "确定要修复 iMessage/IDS/Push 相关文件权限吗？",
                QMessageBox.Yes | QMessageBox.No,
            )
            if reply == QMessageBox.No:
                return
        log("INFO", 7015, "run_permission_fix", "开始修复权限...")
        thread = threading.Thread(target=self._run_permission_fix_thread, daemon=True)
        thread.start()

    # endregion

    # region 激活诊断函数

    def run_diagnose(self, source="local"):
        self._call_source = source
        if source == "remote":
            self._remote_log("开始执行: 系统检测")
        else:
            try:
                if hasattr(self.main_window, "system_log"):
                    self.main_window.system_log("开始执行: 系统检测")
            except Exception:
                pass
            try:
                log("INFO", 7084, "run_diagnose", "开始执行: 系统检测")
            except Exception:
                pass
        if sys.platform != "darwin":
            if source == "local":
                self.show_message_box(
                    QMessageBox.Warning, "提示", "此功能仅在 macOS 系统上可用"
                )
            else:
                log("ERRO", 7026, "run_diagnose", "此功能仅在 macOS 系统上可用")
                self._remote_log("⚠️ 系统检测仅支持 macOS")
            return
        try:
            date_str = datetime.now().strftime("%m%d")
            timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
            logfile = os.path.join(self.reports_dir, f"Diag_IM{date_str}.log")
            self.last_system_diag_report = logfile
            script_content = """#!/bin/bash
echo "===== iMessage 自检工具 ====="
check_process() { pgrep "$1" >/dev/null && echo "1" || echo "0"; }
check_lockdown() { [ -d "/private/var/db/lockdown" ] && ls /private/var/db/lockdown >/dev/null 2>&1 && echo "1" || echo "0"; }
check_ping() { ping -c 1 init.itunes.apple.com >/dev/null 2>&1 && echo "1" || echo "0"; }
check_logs() { log show --last 5m --style syslog --predicate 'subsystem == "com.apple.imfoundation" OR eventMessage CONTAINS "iMessage" OR eventMessage CONTAINS "apsd" OR eventMessage CONTAINS "IDS" OR eventMessage CONTAINS "activation"' 2>/dev/null | grep -Ei "fail|error|denied|timeout|lost|invalid" >/dev/null && echo "0" || echo "1"; }
apsd_ok=$(check_process "apsd")
imagent_ok=$(check_process "imagent")
ids_ok=$(check_process "identityservicesd")
lockdown_ok=$(check_lockdown)
ping_ok=$(check_ping)
logs_ok=$(check_logs)
echo "===== 检测结果 ====="
[[ $apsd_ok == 1 ]] && echo "✔ APS 推送服务进程正常（apsd）" || echo "✘ APS 推送服务未运行"
[[ $imagent_ok == 1 ]] && echo "✔ iMessage 服务进程正常（imagent）" || echo "✘ imagent 未运行"
[[ $ids_ok == 1 ]] && echo "✔ Apple ID / 激活服务正常（identityservicesd）" || echo "✘ identityservicesd 未运行"
[[ $lockdown_ok == 1 ]] && echo "✔ 权限正常（/private/var/db/lockdown 可访问）" || echo "✘ lockdown 权限异常"
[[ $ping_ok == 1 ]] && echo "✔ 苹果激活服务器连接正常" || echo "✘ 无法连接激活服务器"
[[ $logs_ok == 1 ]] && echo "✔ 日志正常：没有激活失败、没有推送错误" || echo "✘ 日志发现可能的失败信息（网络/激活/APS）"
echo "===== 建议动作 ====="
echo "killall apsd"
echo "killall imagent"
echo "killall identityservicesd"
echo "===== 完成 ====="
"""
            timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
            script_file = os.path.join(
                tempfile.gettempdir(), f"imessage_diag_{timestamp_str}.sh"
            )
            with open(script_file, "w", encoding="utf-8") as f:
                f.write(script_content)
            os.chmod(script_file, 0o755)
            ret, out, err = self.run_cmd_local(f'bash "{script_file}"')
            with open(logfile, "w", encoding="utf-8") as f:
                f.write("=" * 60 + "\n")
                f.write("🎯 iMessage 诊断报告\n")
                f.write("=" * 60 + "\n")
                f.write(f"诊断时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n")
                f.write(f"操作系统: macOS\n")
                f.write(f"日志文件: {logfile}\n\n")
                if out:
                    f.write(out)
                if err:
                    f.write(f"\n错误输出:\n{err}\n")
                f.write("\n诊断完成！\n")
            try:
                os.remove(script_file)
            except:
                pass
            log("INFO", 7085, "run_diagnose", f"诊断已完成，报告: {logfile}")
            if source == "local":
                subprocess.run(["open", logfile])
                self.show_message_box(
                    QMessageBox.Information,
                    "完成",
                    f"诊断报告已生成！\n\n文件位置: {logfile}\n已自动打开文件查看",
                )
        except Exception as e:
            log("ERRO", 7098, "run_diagnose", f"执行诊断出错: {str(e)}")
            if source == "local":
                self.show_message_box(
                    QMessageBox.Critical, "错误", f"执行诊断时出错: {str(e)}"
                )

    # endregion

    # region 一键硬核修复（使用线程）

    def run_hard_reset(self, source="local"):
        self._call_source = source
        if source == "remote":
            self._remote_log("开始执行: 超级修复")
        else:
            try:
                if hasattr(self.main_window, "system_log"):
                    self.main_window.system_log("开始执行: 超级修复")
            except Exception:
                pass
            try:
                log("INFO", 7129, "run_hard_reset", "开始执行: 超级修复")
            except Exception:
                pass
        if sys.platform != "darwin":
            if source == "local":
                self.show_message_box(
                    QMessageBox.Warning, "提示", "此功能仅在 macOS 系统上可用"
                )
            else:
                log("ERRO", 7108, "run_hard_reset", "此功能仅在 macOS 系统上可用")
                self._remote_log("⚠️ 超级修复仅支持 macOS")
            return
        if source == "local":
            reply = self.show_message_box(
                QMessageBox.Warning,
                "警告",
                "此操作会删除 Messages、Caches、Preferences 等 iMessage 相关数据（不可恢复）。\n是否继续？",
                QMessageBox.Yes | QMessageBox.No,
            )
            if reply == QMessageBox.No:
                return
            reply2 = self.show_message_box(
                QMessageBox.Warning,
                "最后确认",
                "确定进行超级修复？\n此操作将删除所有 iMessage 相关数据并重新初始化服务。",
                QMessageBox.Yes | QMessageBox.No,
            )
            if reply2 == QMessageBox.No:
                return

        log("INFO", 7130, "run_hard_reset", "开始一键硬核修复...")
        thread = threading.Thread(target=self._run_hard_reset_thread, daemon=True)
        thread.start()

        if source == "local":
            QTimer.singleShot(
                100,
                lambda: self.show_message_box(
                    QMessageBox.Information,
                    "超级修复进行中",
                    "正在执行超级修复...\n\n修复完成后会显示结果提示。\n请稍候...",
                ),
            )

    def _run_hard_reset_thread(self):
        try:
            HOME = os.path.expanduser("~")
            hard_reset_script = f"""#!/bin/bash
set -e
pkill -9 apsd 2>/dev/null || true
pkill -9 imagent 2>/dev/null || true
pkill -9 identityservicesd 2>/dev/null || true
pkill -9 ids 2>/dev/null || true
pkill -9 assistantd 2>/dev/null || true
rm -rf /Library/Preferences/com.apple.apsd.plist 2>/dev/null || true
rm -rf /Library/Preferences/com.apple.ids.service* 2>/dev/null || true
rm -rf /Library/Preferences/com.apple.imfoundation* 2>/dev/null || true
rm -rf "{HOME}/Library/Preferences/com.apple.iChat*" 2>/dev/null || true
rm -rf "{HOME}/Library/Preferences/com.apple.imessage*" 2>/dev/null || true
rm -rf "{HOME}/Library/Preferences/com.apple.ids.service*" 2>/dev/null || true
rm -rf "{HOME}/Library/Preferences/com.apple.identityservices*" 2>/dev/null || true
rm -rf "{HOME}/Library/Preferences/com.apple.imfoundation*" 2>/dev/null || true
rm -rf "{HOME}/Library/Preferences/com.apple.FaceTime*" 2>/dev/null || true
rm -rf "{HOME}/Library/Messages" 2>/dev/null || true
rm -rf "{HOME}/Library/Caches/com.apple.Messages" 2>/dev/null || true
rm -rf "{HOME}/Library/Caches/com.apple.apsd" 2>/dev/null || true
rm -rf "{HOME}/Library/Caches/com.apple.imfoundation*" 2>/dev/null || true
rm -rf "{HOME}/Library/Caches/com.apple.identityservices*" 2>/dev/null || true
rm -rf "{HOME}/Library/Caches/com.apple.ids*" 2>/dev/null || true
rm -rf "{HOME}/Library/IdentityServices" 2>/dev/null || true
rm -rf /private/var/db/crls/* 2>/dev/null || true
rm -rf /private/var/folders/*/*/*/com.apple.aps* 2>/dev/null || true
rm -rf /private/var/folders/*/*/*/com.apple.imfoundation* 2>/dev/null || true
rm -rf /private/var/folders/*/*/*/com.apple.ids* 2>/dev/null || true
/usr/bin/dscacheutil -flushcache 2>/dev/null || true
/usr/bin/killall -HUP mDNSResponder 2>/dev/null || true
launchctl bootout system /System/Library/LaunchDaemons/com.apple.apsd.plist 2>/dev/null || true
launchctl bootstrap system /System/Library/LaunchDaemons/com.apple.apsd.plist 2>/dev/null || true
launchctl bootout gui/$UID /System/Library/LaunchAgents/com.apple.imagent.plist 2>/dev/null || true
launchctl bootstrap gui/$UID /System/Library/LaunchAgents/com.apple.imagent.plist 2>/dev/null || true
launchctl bootout gui/$UID /System/Library/LaunchAgents/com.apple.identityservicesd.plist 2>/dev/null || true
launchctl bootstrap gui/$UID /System/Library/LaunchAgents/com.apple.identityservicesd.plist 2>/dev/null || true
launchctl kickstart -k system/com.apple.apsd 2>/dev/null || true
launchctl kickstart -k gui/$UID/com.apple.imagent 2>/dev/null || true
launchctl kickstart -k gui/$UID/com.apple.identityservicesd 2>/dev/null || true
"""
            fd, path = tempfile.mkstemp(suffix=".sh", text=True)
            with os.fdopen(fd, "w") as f:
                f.write(hard_reset_script)
            os.chmod(path, 0o755)
            ret, out, err = self.run_with_auth(f'"{path}"')
            if ret == 0:
                result_msg = "✅ 超级修复已完成，请重新登录 iMessage"
            else:
                result_msg = f"❌ 超级修复执行出错，退出码: {ret}"
            log("INFO", 7201, "_run_hard_reset_thread", result_msg)
            if self._call_source == "local":
                QTimer.singleShot(
                    0,
                    lambda: self.show_message_box(
                        QMessageBox.Information, "完成", result_msg
                    ),
                )
        except Exception as e:
            error_msg = f"超级修复出错: {str(e)}"
            log("ERRO", 7222, "_run_hard_reset_thread", error_msg)
            if self._call_source == "local":
                QTimer.singleShot(
                    0,
                    lambda: self.show_message_box(QMessageBox.Critical, "错误", error_msg),
                )
            return
        reply = self.show_message_box(
            QMessageBox.Warning,
            "警告",
            "警告：此操作会删除 Messages、Caches、Preferences 等 iMessage 相关所有数据（不可恢复）。\n不会退出 Apple ID。\n是否继续？",
            QMessageBox.Yes | QMessageBox.No,
        )
        if reply == QMessageBox.No:
            return
        reply2 = self.show_message_box(
            QMessageBox.Warning,
            "最后确认",
            "最后一次确认：确定进行超级修复？\n\n此操作将删除所有 iMessage 相关数据并重新初始化服务。",
            QMessageBox.Yes | QMessageBox.No,
        )
        if reply2 == QMessageBox.No:
            return

        if hasattr(self.main_window, "system_log"):
            self.main_window.system_log("开始：一键硬核修复...")

        # 在后台线程中运行
        thread = threading.Thread(target=self._run_hard_reset_thread, daemon=True)
        thread.start()

        # 显示正在修复的提示（非阻塞，使用 QTimer 在主线程中显示）
        QTimer.singleShot(
            100,
            lambda: self.show_message_box(
                QMessageBox.Information,
                "超级修复进行中",
                "正在执行超级修复...\n\n"
                "正在执行的操作：\n"
                "• 清理所有 iMessage 数据和配置\n"
                "• 清理缓存文件\n"
                "• 重启相关服务\n\n"
                "修复完成后会显示结果提示。\n"
                "请稍候...",
            ),
            )

    # endregion

    # region 数据库诊断和修复

    def _find_messages_database(self):
        """尝试找到 Messages 数据库文件"""
        possible_paths = [
            os.path.expanduser("~/Library/Messages/chat.db"),
            os.path.expanduser(
                "~/Library/Containers/com.apple.iChat/Data/Library/Messages/chat.db"
            ),
        ]

        # 检查是否有其他可能的路径
        home = os.path.expanduser("~")
        if home:
            # 检查各种容器路径
            containers_base = os.path.join(home, "Library", "Containers")
            if os.path.exists(containers_base):
                for container in [
                    "com.apple.iChat",
                    "com.apple.MobileSMS",
                    "com.apple.Messages",
                ]:
                    container_path = os.path.join(
                        containers_base,
                        container,
                        "Data",
                        "Library",
                        "Messages",
                        "chat.db",
                    )
                    if os.path.exists(container_path):
                        possible_paths.append(container_path)

            # 检查是否有其他 Messages 相关目录
            messages_dir = os.path.join(home, "Library", "Messages")
            if os.path.exists(messages_dir):
                try:
                    for item in os.listdir(messages_dir):
                        item_path = os.path.join(messages_dir, item)
                        if os.path.isfile(item_path) and item.endswith(".db"):
                            if item_path not in possible_paths:
                                possible_paths.append(item_path)
                except PermissionError:
                    pass

        found = []
        for path in possible_paths:
            if os.path.exists(path):
                try:
                    size = os.path.getsize(path)
                    found.append((path, size))
                except (PermissionError, OSError):
                    found.append((path, -1))  # -1 表示无法访问

        return found

    def _check_database(self, path):
        """检查数据库文件"""
        info = {
            "path": path,
            "exists": os.path.exists(path),
            "size": 0,
            "readable": False,
            "has_message_table": False,
            "all_tables": [],
        }

        if info["exists"]:
            info["size"] = os.path.getsize(path)

            if info["size"] > 0:
                try:
                    conn = sqlite3.connect(path)
                    cursor = conn.cursor()
                    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                    info["all_tables"] = [row[0] for row in cursor.fetchall()]
                    info["has_message_table"] = "message" in info["all_tables"]
                    info["readable"] = True
                    conn.close()
                except Exception as e:
                    info["error"] = str(e)

        return info

    def _fix_database_permissions(self, path):
        """尝试修复数据库文件权限"""
        try:
            os.chmod(path, 0o644)
            return True, "权限修复成功"
        except PermissionError:
            return False, "需要管理员权限"
        except Exception as e:
            return False, f"权限修复失败: {str(e)}"

    def _repair_database(self, path):
        """尝试修复损坏的数据库（VACUUM 和 REINDEX）"""
        try:
            # 先备份（安全措施）
            if os.path.exists(path):
                timestamp = int(time.time())
                backup_path = path + f".backup_{timestamp}"
                try:
                    shutil.copy2(path, backup_path)
                except Exception as e:
                    return False, f"备份失败: {str(e)}", None
            else:
                return False, "数据库文件不存在", None

            # 尝试连接数据库
            try:
                conn = sqlite3.connect(path)
                cursor = conn.cursor()
            except sqlite3.DatabaseError as e:
                return (
                    False,
                    f"数据库损坏，无法连接: {str(e)}。建议从 Time Machine 恢复或重新初始化 iMessage。",
                    backup_path,
                )

            try:
                # 执行完整性检查
                cursor.execute("PRAGMA integrity_check")
                integrity_result = cursor.fetchone()

                if integrity_result and integrity_result[0] == "ok":
                    # 数据库完整，执行优化
                    cursor.execute("VACUUM")
                    cursor.execute("REINDEX")
                    conn.commit()
                    conn.close()
                    return True, "数据库优化完成（已创建备份）", backup_path
                else:
                    conn.close()
                    return (
                        False,
                        f"数据库完整性检查失败: {integrity_result[0] if integrity_result else '未知错误'}。建议从备份恢复。",
                        backup_path,
                    )
            except sqlite3.DatabaseError as e:
                conn.close()
                return (
                    False,
                    f"数据库操作失败: {str(e)}。备份已保存: {backup_path}",
                    backup_path,
                )
        except Exception as e:
            return False, f"修复过程出错: {str(e)}", None

    def run_database_diagnose(self, source="local"):
        self._call_source = source
        if source == "remote":
            self._remote_log("开始执行: 数据库检测")
        else:
            try:
                if hasattr(self.main_window, "system_log"):
                    self.main_window.system_log("开始执行: 数据库检测")
            except Exception:
                pass
            try:
                log("INFO", 7582, "run_database_diagnose", "开始执行: 数据库检测")
            except Exception:
                pass
        if sys.platform != "darwin":
            if source == "local":
                self.show_message_box(
                    QMessageBox.Warning, "提示", "此功能仅在 macOS 系统上可用"
                )
            else:
                log("ERRO", 7488, "run_database_diagnose", "此功能仅在 macOS 系统上可用")
                self._remote_log("⚠️ 数据库检测仅支持 macOS")
            return

        try:
            found_files = self._find_messages_database()

            if not found_files:
                log("ERRO", 7498, "run_database_diagnose", "未找到 Messages 数据库文件")
                if source == "local":
                    self.show_message_box(
                        QMessageBox.Warning,
                        "诊断结果",
                        "❌ 未找到任何 Messages 数据库文件\n\n可能从未使用过 iMessage",
                    )
                return

            report_lines = []
            report_lines.append("=" * 60)
            report_lines.append("macOS Messages 数据库诊断报告")
            report_lines.append("=" * 60)
            report_lines.append(
                f"诊断时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n"
            )

            fixable_issues = []

            for path, size in found_files:
                report_lines.append(f"\n📁 {path}")

                if size == -1:
                    report_lines.append(
                        "   ⚠️  无法访问 - 请检查是否授予完全磁盘访问权限 (Full Disk Access)"
                    )
                    fixable_issues.append(("permission", path))
                elif size == 0:
                    report_lines.append("   ⚠️  文件为空（0字节）")
                    report_lines.append(
                        "   提示: 需要先使用 iMessage 发送/接收消息来初始化数据库"
                    )
                else:
                    size_mb = size / 1024 / 1024
                    report_lines.append(f"   大小: {size} 字节 ({size_mb:.2f} MB)")
                    info = self._check_database(path)
                    if info["readable"]:
                        report_lines.append(f"   ✅ 可读取")
                        report_lines.append(f"   表数量: {len(info['all_tables'])}")
                        if info["has_message_table"]:
                            report_lines.append(f"   ✅ 包含 'message' 表（可以使用）")
                        else:
                            report_lines.append(f"   ❌ 不包含 'message' 表")
                            if info["all_tables"]:
                                report_lines.append(
                                    f"   数据库中的表: {', '.join(info['all_tables'][:10])}"
                                )
                    else:
                        report_lines.append(f"   ❌ 无法读取")
                        if "error" in info:
                            error_msg = info["error"]
                            report_lines.append(f"   错误: {error_msg}")
                            if (
                                "database" in error_msg.lower()
                                or "corrupt" in error_msg.lower()
                                or "locked" in error_msg.lower()
                            ):
                                fixable_issues.append(("corrupt", path))
                            else:
                                fixable_issues.append(("permission", path))

            valid_files = [
                (f[0], f[1]) for f in found_files if f[1] > 0 and os.path.exists(f[0])
            ]
            if valid_files:
                for path, size in valid_files:
                    info = self._check_database(path)
                    if info.get("has_message_table"):
                        report_lines.append("\n" + "=" * 60)
                        report_lines.append(f"✅ 推荐使用: {path}")
                        report_lines.append("=" * 60)
                        break

            date_str = datetime.now().strftime("%m%d")
            logfile = os.path.join(self.reports_dir, f"Log_DateBase{date_str}.log")
            self.last_db_diag_report = logfile
            with open(logfile, "w", encoding="utf-8") as f:
                f.write("\n".join(report_lines))

            log("INFO", 7583, "run_database_diagnose", f"数据库诊断已完成，报告: {logfile}")

            if source == "local":
                summary_lines = report_lines[:15]
                summary_text = "\n".join(summary_lines)
                if len(report_lines) > 15:
                    summary_text += "\n\n... (更多内容请查看完整报告文件)"

                if fixable_issues:
                    reply = self.show_message_box(
                        QMessageBox.Question,
                        "诊断完成",
                        f"{summary_text}\n\n"
                        f"发现 {len(fixable_issues)} 个可修复的问题。\n\n"
                        f"完整报告已保存到: {logfile}\n"
                        f"是否尝试自动修复？",
                        QMessageBox.Yes | QMessageBox.No,
                    )

                    if reply == QMessageBox.Yes:
                        self._fix_database_issues(fixable_issues)
                else:
                    self.show_message_box(
                        QMessageBox.Information,
                        "诊断完成",
                        f"{summary_text}\n\n" f"完整报告已保存到: {logfile}",
                    )

                if os.path.exists(logfile):
                    subprocess.run(["open", logfile])

        except Exception as e:
            log("ERRO", 7621, "run_database_diagnose", f"数据库诊断出错: {str(e)}")
            if source == "local":
                self.show_message_box(
                    QMessageBox.Critical, "错误", f"执行数据库诊断时出错: {str(e)}"
                )

    def _fix_database_issues(self, fixable_issues):
        """修复数据库问题"""
        results = []
        for issue_type, path in fixable_issues:
            if issue_type == "permission":
                success, message = self._fix_database_permissions(path)
                if success:
                    results.append(f"✅ {path}: {message}")
                else:
                    results.append(f"❌ {path}: {message}")
                    if "管理员权限" in message:
                        results.append("   提示: 请使用管理员权限运行")
            elif issue_type == "corrupt":
                success, message, backup = self._repair_database(path)
                if success:
                    results.append(f"✅ {path}: {message}")
                    if backup:
                        results.append(f"   📦 备份文件: {backup}")
                else:
                    results.append(f"❌ {path}: {message}")
                    if backup:
                        results.append(f"   📦 备份文件: {backup}")

        result_text = "\n".join(results)
        self.show_message_box(
            QMessageBox.Information,
            "修复完成",
            f"{result_text}\n\n" f"建议重新运行诊断以确认问题已解决。",
        )

    # endregion

    # region 清空收件箱（使用线程）

    def _clear_imessage_inbox_thread(self):
        try:
            HOME = os.path.expanduser("~")
            subprocess.run(["pkill", "Messages"], stderr=subprocess.DEVNULL)
            time.sleep(1)

            db_path = os.path.join(HOME, "Library/Messages/chat.db")
            attachments_path = os.path.join(HOME, "Library/Messages/Attachments")
            if os.path.exists(db_path):
                conn = sqlite3.connect(db_path)
                c = conn.cursor()
                c.execute("DELETE FROM message")
                c.execute("DELETE FROM chat")
                c.execute("DELETE FROM chat_message_join")
                conn.commit()
                conn.close()
            if os.path.exists(attachments_path):
                shutil.rmtree(attachments_path)
            log("INFO", 7676, "_clear_imessage_inbox_thread", "收件箱已清空")
            if self._call_source == "local":
                QTimer.singleShot(
                    0,
                    lambda: self.show_message_box(
                        QMessageBox.Information, "完成", "✅ 收件箱已清空"
                    ),
                )
        except Exception as e:
            error_msg = f"清空收件箱失败: {str(e)}"
            log("ERRO", 7690, "_clear_imessage_inbox_thread", error_msg)
            if self._call_source == "local":
                QTimer.singleShot(
                    0,
                    lambda: self.show_message_box(QMessageBox.Critical, "错误", error_msg),
                )

    def clear_imessage_inbox(self, source="local"):
        self._call_source = source
        if source == "remote":
            self._remote_log("开始执行: 清空收件箱")
        else:
            try:
                if hasattr(self.main_window, "system_log"):
                    self.main_window.system_log("开始执行: 清空收件箱")
            except Exception:
                pass
            try:
                log("INFO", 7708, "clear_imessage_inbox", "开始执行: 清空收件箱")
            except Exception:
                pass
        if source == "local":
            reply = self.show_message_box(
                QMessageBox.Warning,
                "警告",
                "此操作将删除所有 iMessage 聊天记录及附件，不可恢复。继续？",
                QMessageBox.Yes | QMessageBox.No,
            )
            if reply == QMessageBox.No:
                return
        log("INFO", 7709, "clear_imessage_inbox", "开始清空收件箱...")
        thread = threading.Thread(target=self._clear_imessage_inbox_thread, daemon=True)
        thread.start()

    # endregion

# endregion

# region *****************************     ending                ********************************


if __name__ == "__main__":

    threading.Thread(target=start_async_backend, daemon=True).start()
    time.sleep(0.5) 

    QApplication.setAttribute(Qt.AA_EnableHighDpiScaling, True)
    QApplication.setAttribute(Qt.AA_UseHighDpiPixmaps, True)
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    
    set_signals(window) 
    atexit.register(stop_log)

    sys.exit(app.exec_())

    
# endregion

