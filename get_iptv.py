import requests
import pandas as pd
import re
import os
import time
import logging
import json
import stat
import platform
from itertools import cycle
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# ======================== 核心配置区 =========================
# 基础功能配置
SOURCE_URLS = [
    "https://raw.githubusercontent.com/zwc456baby/iptv_alive/master/live.txt",
    "https://live.zbds.top/tv/iptv6.txt",
    "https://live.zbds.top/tv/iptv4.txt",
]
DEFAULT_TEMPLATE = "demo.txt"  # 默认分类模板
BACKUP_TEMPLATE = "demo_backup.txt"  # 备份模板
MAX_INTERFACES_PER_CHANNEL = 5  # 单频道最多保留接口数
SPEED_TEST_TIMEOUT = 8  # 测速超时时间（秒）
MAX_SPEED_TEST_WORKERS = 15  # 测速并发线程数
MAX_FETCH_WORKERS = 5  # 源抓取并发线程数

# 输出配置（固定文件名）
TXT_OUTPUT = "iptv.txt"
M3U_OUTPUT = "iptv.m3u"
CATEGORY_MARKER = "#genre#"  # 模板中分类标记
CACHE_FILE = ".iptv_valid_cache.json"  # 源有效性缓存文件
CACHE_EXPIRE = 3600  # 缓存过期时间（秒）
MAX_CACHE_SIZE = 100  # 最大缓存数量（动态调整）

# 抓取优化配置
MAX_REDIRECTS = 3  # 最大重定向次数
REQ_INTERVAL = [0.2, 0.3, 0.4, 0.5]  # 抓取请求间隔（随机循环）
MIN_CONTENT_LEN = 100  # 有效源内容最小长度（字符）
TEST_URL = "https://www.baidu.com"  # 网络检测URL

# 系统兼容性配置
SYSTEM = platform.system()
IS_WINDOWS = SYSTEM == "Windows"
IS_LINUX = SYSTEM == "Linux"
IS_MAC = SYSTEM == "Darwin"

# 颜色输出配置（Windows终端兼容）
if IS_WINDOWS:
    COLOR_GREEN = ""
    COLOR_RED = ""
    COLOR_YELLOW = ""
    COLOR_BLUE = ""
    COLOR_RESET = ""
else:
    COLOR_GREEN = "\033[92m"
    COLOR_RED = "\033[91m"
    COLOR_YELLOW = "\033[93m"
    COLOR_BLUE = "\033[94m"
    COLOR_RESET = "\033[0m"

# 线程安全锁
CACHE_LOCK = Lock()  # 缓存操作锁
PRINT_LOCK = Lock()  # 控制台输出锁
# =========================================================================

# 正则表达式定义
IPV4_PAT = re.compile(r'^https?://(\d{1,3}\.){3}\d{1,3}')
IPV6_PAT = re.compile(r'^https?://\[([a-fA-F0-9:]+)\]')
URL_PAT = re.compile(r'^https?://')
SPACE_CLEAN_PAT = re.compile(r'\s+')

# 日志初始化（分级记录，便于排查）
logging.basicConfig(
    filename="iptv_tool.log",
    level=logging.INFO,
    encoding="utf-8",
    format="%(asctime)s - %(levelname)s - %(module)s:%(lineno)d - %(message)s"
)


def print_sep(title: str = "", length: int = 70) -> None:
    """打印带标题的分隔线，线程安全"""
    with PRINT_LOCK:
        sep = "=" * length
        if title:
            print(f"\n{sep}\n📌 {COLOR_BLUE}{title}{COLOR_RESET}\n{sep}")
        else:
            print(sep)


def safe_print(msg: str) -> None:
    """线程安全的控制台输出"""
    with PRINT_LOCK:
        print(msg)


def clean_text(text: str) -> str:
    """清理文本：去除多余空格、换行符"""
    return SPACE_CLEAN_PAT.sub("", str(text).strip())


def check_network() -> bool:
    """检测网络连接状态，适配多系统"""
    safe_print(f"{COLOR_BLUE}🔍 正在检测网络连接...{COLOR_RESET}")
    try:
        timeout = 3 if not IS_WINDOWS else 5
        resp = requests.get(TEST_URL, timeout=timeout)
        if resp.status_code == 200:
            safe_print(f"{COLOR_GREEN}✅ 网络连接正常（{SYSTEM}系统）{COLOR_RESET}")
            return True
        else:
            safe_print(f"{COLOR_RED}❌ 网络检测失败：HTTP状态码 {resp.status_code}{COLOR_RESET}")
            return False
    except Exception as e:
        safe_print(f"{COLOR_RED}❌ 网络连接异常：{str(e)}{COLOR_RESET}")
        return False


def set_file_permissions(file_path: str) -> None:
    """设置文件权限，适配多系统"""
    if IS_WINDOWS:
        return
    try:
        os.chmod(file_path, stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH)
        logging.info(f"设置文件权限：{file_path}")
    except Exception as e:
        safe_print(f"{COLOR_YELLOW}⚠️ 设置文件权限失败：{str(e)}{COLOR_RESET}")
        logging.warning(f"文件权限设置失败：{file_path} - {str(e)}")


def generate_default_template() -> bool:
    """生成默认分类模板（当模板文件不存在时）"""
    default_cats = [
        {"name": "央视频道", "channels": ["CCTV1", "CCTV2", "CCTV3", "CCTV5", "CCTV6", "CCTV8", "CCTV13"]},
        {"name": "卫视频道", "channels": ["湖南卫视", "浙江卫视", "东方卫视", "江苏卫视", "北京卫视", "安徽卫视"]},
        {"name": "地方频道", "channels": ["广东卫视", "山东卫视", "四川卫视", "湖北卫视", "河南卫视"]}
    ]
    try:
        with open(DEFAULT_TEMPLATE, 'w', encoding='utf-8') as f:
            f.write(f"# IPTV分类模板（自动生成于 {time.strftime('%Y-%m-%d %H:%M:%S')}）\n")
            f.write(f"# 系统：{SYSTEM} | 格式：{CATEGORY_MARKER} 分类名 换行 频道名\n\n")
            for cat in default_cats:
                f.write(f"{CATEGORY_MARKER} {cat['name']}\n")
                for ch in cat["channels"]:
                    f.write(f"{ch}\n")
                f.write("\n")
        set_file_permissions(DEFAULT_TEMPLATE)
        safe_print(f"{COLOR_GREEN}✅ 默认模板生成成功：{os.path.abspath(DEFAULT_TEMPLATE)}{COLOR_RESET}")
        return True
    except Exception as e:
        safe_print(f"{COLOR_RED}❌ 生成默认模板失败：{str(e)}{COLOR_RESET}")
        logging.error(f"模板生成失败：{str(e)}")
        return False


def read_template(template_path: str = DEFAULT_TEMPLATE) -> tuple[list[dict], list[str]] | tuple[None, None]:
    """读取分类模板，支持指定路径、自动备份和生成"""
    if not os.path.exists(template_path):
        safe_print(f"{COLOR_YELLOW}⚠️ 模板「{template_path}」不存在，生成默认模板...{COLOR_RESET}")
        if not generate_default_template():
            return None, None

    # 自动备份模板
    try:
        with open(template_path, 'r', encoding='utf-8') as f_src, open(BACKUP_TEMPLATE, 'w', encoding='utf-8') as f_dst:
            f_dst.write(f"# 模板备份（{time.strftime('%Y-%m-%d %H:%M:%S')}）\n# 源模板：{template_path}\n")
            f_dst.write(f_src.read())
        set_file_permissions(BACKUP_TEMPLATE)
    except Exception as e:
        safe_print(f"{COLOR_YELLOW}⚠️ 模板备份失败：{str(e)}，不影响流程{COLOR_RESET}")
        logging.warning(f"模板备份失败：{str(e)}")

    categories = []
    current_cat = None
    all_channels = []

    try:
        with open(template_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                if line.startswith("#") and not line.startswith(CATEGORY_MARKER):
                    continue

                # 处理分类行
                if line.startswith(CATEGORY_MARKER):
                    cat_name = clean_text(line.lstrip(CATEGORY_MARKER))
                    if not cat_name:
                        safe_print(f"{COLOR_YELLOW}⚠️ 第{line_num}行：分类名为空，忽略{COLOR_RESET}")
                        current_cat = None
                        continue
                    existing = next((c for c in categories if c["name"] == cat_name), None)
                    if existing:
                        current_cat = cat_name
                    else:
                        categories.append({"name": cat_name, "channels": []})
                        current_cat = cat_name
                    continue

                # 处理频道行
                if current_cat is None:
                    safe_print(f"{COLOR_YELLOW}⚠️ 第{line_num}行：频道未分类，归入「未分类」{COLOR_RESET}")
                    if not any(c["name"] == "未分类" for c in categories):
                        categories.append({"name": "未分类", "channels": []})
                    current_cat = "未分类"

                ch_name = clean_text(line.split(",")[0])
                if not ch_name:
                    safe_print(f"{COLOR_YELLOW}⚠️ 第{line_num}行：频道名为空，忽略{COLOR_RESET}")
                    continue
                if ch_name not in all_channels:
                    all_channels.append(ch_name)
                    for cat in categories:
                        if cat["name"] == current_cat:
                            cat["channels"].append(ch_name)
                            break

    except Exception as e:
        safe_print(f"{COLOR_RED}❌ 读取模板失败：{str(e)}{COLOR_RESET}")
        logging.error(f"模板读取失败：{str(e)}")
        return None, None

    # 输出模板统计
    total_ch = sum(len(c["channels"]) for c in categories)
    safe_print(f"{COLOR_GREEN}✅ 模板读取完成 | 分类数：{len(categories)} | 总频道数：{total_ch} | 路径：{os.path.abspath(template_path)}{COLOR_RESET}")
    safe_print("  " + "-" * 70)
    for i, cat in enumerate(categories, 1):
        safe_print(f"  {i:2d}. {cat['name']:<25} 频道数：{len(cat['channels']):2d}")
    safe_print("  " + "-" * 70)
    return categories, all_channels


def load_valid_cache() -> dict:
    """加载缓存，自动清理过期/超量项（线程安全）"""
    with CACHE_LOCK:
        if not os.path.exists(CACHE_FILE):
            return {}
        try:
            with open(CACHE_FILE, 'r', encoding='utf-8') as f:
                cache = json.load(f)
            # 过滤过期缓存
            current_time = time.time()
            valid_cache = {
                url: info for url, info in cache.items()
                if current_time - info["timestamp"] < CACHE_EXPIRE
            }
            # 清理超量缓存
            if len(valid_cache) > MAX_CACHE_SIZE:
                sorted_cache = sorted(valid_cache.items(), key=lambda x: x[1]["timestamp"], reverse=True)
                valid_cache = dict(sorted_cache[:MAX_CACHE_SIZE])
                safe_print(f"{COLOR_YELLOW}⚠️ 缓存超量，保留最新{MAX_CACHE_SIZE}个{COLOR_RESET}")
            logging.info(f"加载缓存：{len(valid_cache)}个有效项")
            return valid_cache
        except Exception as e:
            safe_print(f"{COLOR_YELLOW}⚠️ 加载缓存失败：{str(e)}，重新生成{COLOR_RESET}")
            logging.error(f"缓存加载失败：{str(e)}")
            return {}


def save_valid_cache(cache: dict) -> bool:
    """保存缓存，自动控制大小（线程安全）"""
    with CACHE_LOCK:
        if len(cache) > MAX_CACHE_SIZE:
            sorted_cache = sorted(cache.items(), key=lambda x: x[1]["timestamp"], reverse=True)
            cache = dict(sorted_cache[:MAX_CACHE_SIZE])
        try:
            with open(CACHE_FILE, 'w', encoding='utf-8') as f:
                json.dump(cache, f, ensure_ascii=False, indent=2)
            set_file_permissions(CACHE_FILE)
            logging.info(f"保存缓存：{len(cache)}个项")
            return True
        except Exception as e:
            safe_print(f"{COLOR_YELLOW}⚠️ 保存缓存失败：{str(e)}，不影响流程{COLOR_RESET}")
            logging.error(f"缓存保存失败：{str(e)}")
            return False


def is_valid_url(url: str) -> bool:
    """验证URL是否为HTTP/HTTPS格式"""
    return URL_PAT.match(url) is not None


def fetch_single(url: str, cache: dict) -> str | None:
    """抓取单个源，结合缓存优化（线程安全）"""
    # 检查缓存
    current_time = time.time()
    with CACHE_LOCK:
        if url in cache:
            cache_info = cache[url]
            if current_time - cache_info["timestamp"] < CACHE_EXPIRE:
                if cache_info["valid"]:
                    safe_print(f"{COLOR_BLUE}🔍 缓存命中：{url[:50]}{'...' if len(url)>50 else ''}（有效）{COLOR_RESET}")
                    return cache_info["content"]
                else:
                    safe_print(f"{COLOR_YELLOW}🔍 缓存命中：{url[:50]}{'...' if len(url)>50 else ''}（失效，跳过）{COLOR_RESET}")
                    return None

    safe_print(f"{COLOR_BLUE}🔍 抓取：{url[:50]}{'...' if len(url)>50 else ''}{COLOR_RESET}")
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0.0.0 Safari/537.36",
            "Accept": "*/*",
            "Connection": "keep-alive"
        }
        # 适配系统超时
        connect_timeout = 5 if not IS_WINDOWS else 8
        read_timeout = SPEED_TEST_TIMEOUT if not IS_WINDOWS else SPEED_TEST_TIMEOUT + 2
        resp = requests.get(
            url,
            timeout=(connect_timeout, read_timeout),
            headers=headers,
            allow_redirects=True,
            max_redirects=MAX_REDIRECTS,
            stream=False
        )
        resp.raise_for_status()

        # 处理编码
        if not resp.encoding or resp.encoding.lower() == "iso-8859-1":
            resp.encoding = resp.apparent_encoding
        content = resp.text

        # 校验内容有效性
        if len(content) < MIN_CONTENT_LEN:
            safe_print(f"{COLOR_YELLOW}⚠️ 内容过短（{len(content)}字符），无效{COLOR_RESET}")
            with CACHE_LOCK:
                cache[url] = {"valid": False, "timestamp": current_time}
            return None
        if not (URL_PAT.search(content) or "#EXTM3U" in content[:100]):
            safe_print(f"{COLOR_YELLOW}⚠️ 无直播源信息，无效{COLOR_RESET}")
            with CACHE_LOCK:
                cache[url] = {"valid": False, "timestamp": current_time}
            return None

        # 缓存有效内容
        safe_print(f"{COLOR_GREEN}✅ 抓取成功 | 长度：{len(content):,}字符{COLOR_RESET}")
        with CACHE_LOCK:
            cache[url] = {"valid": True, "content": content, "timestamp": current_time}
        return content

    except requests.exceptions.ConnectTimeout:
        msg = "连接超时"
    except requests.exceptions.ReadTimeout:
        msg = f"读取超时（>{read_timeout}秒）"
    except requests.exceptions.TooManyRedirects:
        msg = f"重定向超{MAX_REDIRECTS}次"
    except requests.exceptions.ConnectionError:
        msg = "网络连接失败"
    except requests.exceptions.HTTPError as e:
