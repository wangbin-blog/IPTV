import requests
import re
import os
import time
import logging
import json
import stat
import platform
import random
from itertools import cycle
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
try:
    from typing import List, Dict, Tuple, Optional, Any
except ImportError:
    # Python 3.8以下兼容
    from typing import List, Dict, Tuple, Optional, Any

# ======================== 核心配置区（按功能分组）=========================
# 1. 基础功能配置
SOURCE_URLS = [
    "https://raw.githubusercontent.com/zwc456baby/iptv_alive/master/live.txt",
    "https://raw.githubusercontent.com/iptv-org/iptv/gh-pages/countries/cn.m3u",
    "https://ghfast.top/raw.githubusercontent.com/Supprise0901/TVBox_live/main/live.txt",
    "https://gh-proxy.com/https://raw.githubusercontent.com/wwb521/live/main/tv.m3u",
    "https://gh-proxy.com/https://raw.githubusercontent.com/zeee-u/lzh06/main/fl.m3u",
    "https://raw.githubusercontent.com/Guovin/iptv-database/master/result.txt",  
    "https://raw.githubusercontent.com/iptv-org/iptv/master/streams/cn.m3u",
    "https://raw.githubusercontent.com/suxuang/myIPTV/main/ipv4.m3u",
    "https://raw.githubusercontent.com/kimwang1978/collect-tv-txt/main/others_output.txt",
    "https://raw.githubusercontent.com/vbskycn/iptv/master/tv/iptv4.txt",
    "http://47.120.41.246:8899/zb.txt",
    "https://live.zbds.top/tv/iptv4.txt",
]
DEFAULT_TEMPLATE = "demo.txt"  # 默认分类模板
BACKUP_TEMPLATE = "demo_backup.txt"  # 模板备份文件
MAX_INTERFACES_PER_CHANNEL = 8  # 单频道最多保留接口数
SPEED_TEST_TIMEOUT = 10  # 基础测速超时（秒）
MAX_SPEED_TEST_WORKERS = 15  # 测速并发线程数
MAX_FETCH_WORKERS = 5  # 源抓取并发线程数

# 2. 输出配置
TXT_OUTPUT = "iptv.txt"  # TXT结果文件
M3U_OUTPUT = "iptv.m3u"  # M3U结果文件（兼容主流播放器）
CATEGORY_MARKER = "#genre#"  # 模板分类标记
UNCATEGORIZED_NAME = "其他频道"  # 未分类频道归属

# 3. 缓存配置
CACHE_FILE = ".iptv_valid_cache.json"  # 缓存文件路径
CACHE_EXPIRE = 3600  # 缓存过期时间（秒）
MAX_CACHE_SIZE = 100  # 最大缓存数量（防止内存溢出）

# 4. 网络请求配置
MAX_REDIRECTS = 3  # 最大重定向次数
REQ_INTERVAL = [0.2, 0.3, 0.4, 0.5]  # 抓取请求间隔（随机循环）
MIN_CONTENT_LEN = 100  # 有效源内容最小长度（字符）
TEST_URL = "https://www.baidu.com"  # 网络连通性检测URL

# 5. 系统兼容性配置
SYSTEM = platform.system()
IS_WINDOWS = SYSTEM == "Windows"
IS_LINUX = SYSTEM == "Linux"
IS_MAC = SYSTEM == "Darwin"

# 6. 终端输出颜色配置（Windows终端兼容）
try:
    if IS_WINDOWS:
        try:
            import colorama
            colorama.init()
            COLOR_GREEN = colorama.Fore.GREEN
            COLOR_RED = colorama.Fore.RED
            COLOR_YELLOW = colorama.Fore.YELLOW
            COLOR_BLUE = colorama.Fore.BLUE
            COLOR_RESET = colorama.Fore.RESET
        except ImportError:
            # 如果没有colorama，在Windows上不使用颜色
            COLOR_GREEN = COLOR_RED = COLOR_YELLOW = COLOR_BLUE = COLOR_RESET = ""
    else:
        COLOR_GREEN = "\033[92m"
        COLOR_RED = "\033[91m"
        COLOR_YELLOW = "\033[93m"
        COLOR_BLUE = "\033[94m"
        COLOR_RESET = "\033[0m"
except Exception:
    # 颜色初始化失败时的回退
    COLOR_GREEN = COLOR_RED = COLOR_YELLOW = COLOR_BLUE = COLOR_RESET = ""

# 7. 线程安全锁
CACHE_LOCK = Lock()  # 缓存操作锁
PRINT_LOCK = Lock()  # 控制台输出锁
# =========================================================================

# ======================== 正则表达式定义 =========================
IPV4_PAT = re.compile(r'^https?://(\d{1,3}\.){3}\d{1,3}')  # IPv4地址匹配
IPV6_PAT = re.compile(r'^https?://\[([a-fA-F0-9:]+)\]')  # IPv6地址匹配
URL_PAT = re.compile(r'^https?://')  # HTTP/HTTPS URL匹配
SPACE_CLEAN_PAT = re.compile(r'^\s+|\s+$|\s+(?=\s)')  # 空格清理（首尾+连续空格）
CHANNEL_PAT = re.compile(r'([^,]+),(https?://.+)$')  # 频道名-URL匹配（源文件格式）
# =========================================================================

# ======================== 日志初始化 =========================
def setup_logging():
    """初始化日志配置"""
    try:
        logging.basicConfig(
            filename="iptv_tool.log",
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(module)s:%(lineno)d - %(message)s"
        )
    except Exception as e:
        print(f"日志初始化失败: {e}")

setup_logging()
# =========================================================================

# ======================== 配置验证函数 =========================
def validate_config():
    """验证配置参数的完整性"""
    config_errors = []
    
    if not SOURCE_URLS:
        config_errors.append("SOURCE_URLS 不能为空")
    
    if MAX_FETCH_WORKERS <= 0:
        config_errors.append("MAX_FETCH_WORKERS 必须大于0")
    
    if MAX_SPEED_TEST_WORKERS <= 0:
        config_errors.append("MAX_SPEED_TEST_WORKERS 必须大于0")
    
    if SPEED_TEST_TIMEOUT <= 0:
        config_errors.append("SPEED_TEST_TIMEOUT 必须大于0")
    
    if not REQ_INTERVAL:
        config_errors.append("REQ_INTERVAL 不能为空")
    
    if config_errors:
        error_msg = "配置验证失败:\n" + "\n".join(f"  - {error}" for error in config_errors)
        safe_print(f"{COLOR_RED}{error_msg}{COLOR_RESET}")
        return False
    
    return True
# =========================================================================

# ======================== 基础工具函数 =========================
def print_sep(title: str = "", length: int = 70) -> None:
    """打印带标题的分隔线（线程安全）"""
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
    if text is None:
        return ""
    return SPACE_CLEAN_PAT.sub("", str(text).strip())


def check_network() -> bool:
    """检测网络连接状态（适配多系统）"""
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
    """设置文件权限（适配多系统，Windows无操作）"""
    if IS_WINDOWS:
        return
    try:
        os.chmod(file_path, stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH)
        logging.info(f"文件权限设置完成：{file_path}")
    except Exception as e:
        safe_print(f"{COLOR_YELLOW}⚠️ 文件权限设置失败：{str(e)}{COLOR_RESET}")
        logging.warning(f"文件权限设置失败：{file_path} - {str(e)}")


def is_valid_url(url: str) -> bool:
    """验证URL是否为HTTP/HTTPS格式"""
    if not url:
        return False
    return URL_PAT.match(url) is not None


def get_random_interval() -> float:
    """获取随机请求间隔"""
    return random.choice(REQ_INTERVAL)


def get_cache_file_path() -> str:
    """获取缓存文件完整路径"""
    return os.path.abspath(CACHE_FILE)
# =========================================================================

# ======================== 模板处理函数 =========================
def generate_default_template() -> bool:
    """生成默认分类模板（格式：分类名,#genre# 换行 频道名）"""
    default_categories = [
        {
            "name": "央视频道",
            "marker": f"央视频道,{CATEGORY_MARKER}",
            "channels": ["CCTV1", "CCTV2", "CCTV3", "CCTV5", "CCTV6", "CCTV8", "CCTV13", "CCTV14", "CCTV15"]
        },
        {
            "name": "卫视频道",
            "marker": f"卫视频道,{CATEGORY_MARKER}",
            "channels": ["湖南卫视", "浙江卫视", "东方卫视", "江苏卫视", "北京卫视", "安徽卫视", "深圳卫视", "山东卫视"]
        },
        {
            "name": "地方频道",
            "marker": f"地方频道,{CATEGORY_MARKER}",
            "channels": ["广东卫视", "四川卫视", "湖北卫视", "河南卫视", "河北卫视", "辽宁卫视", "黑龙江卫视"]
        },
        {
            "name": UNCATEGORIZED_NAME,
            "marker": f"{UNCATEGORIZED_NAME},{CATEGORY_MARKER}",
            "channels": []
        }
    ]
    try:
        with open(DEFAULT_TEMPLATE, 'w', encoding='utf-8') as f:
            f.write(f"# IPTV分类模板（自动生成于 {time.strftime('%Y-%m-%d %H:%M:%S')}）\n")
            f.write(f"# 系统：{SYSTEM} | 格式说明：分类行（分类名,{CATEGORY_MARKER}）、频道行（纯频道名）\n")
            f.write(f"# 未匹配模板的频道将自动归入「{UNCATEGORIZED_NAME}」\n\n")
            for cat in default_categories:
                f.write(f"{cat['marker']}\n")
                for channel in cat["channels"]:
                    f.write(f"{channel}\n")
                f.write("\n")
        set_file_permissions(DEFAULT_TEMPLATE)
        safe_print(f"{COLOR_GREEN}✅ 默认模板生成成功：{os.path.abspath(DEFAULT_TEMPLATE)}{COLOR_RESET}")
        return True
    except Exception as e:
        safe_print(f"{COLOR_RED}❌ 生成默认模板失败：{str(e)}{COLOR_RESET}")
        logging.error(f"模板生成失败：{str(e)}")
        return False


def read_template(template_path: str = DEFAULT_TEMPLATE):
    """读取分类模板，返回分类列表和所有频道名列表"""
    if not os.path.exists(template_path):
        safe_print(f"{COLOR_YELLOW}⚠️ 分类模板不存在，自动生成...{COLOR_RESET}")
        if not generate_default_template():
            return None, None

    # 自动备份模板
    try:
        with open(template_path, 'r', encoding='utf-8') as f_src, open(BACKUP_TEMPLATE, 'w', encoding='utf-8') as f_dst:
            f_dst.write(f"# 模板备份（{time.strftime('%Y-%m-%d %H:%M:%S')}）\n# 源路径：{os.path.abspath(template_path)}\n")
            f_dst.write(f_src.read())
        set_file_permissions(BACKUP_TEMPLATE)
    except Exception as e:
        safe_print(f"{COLOR_YELLOW}⚠️ 模板备份失败：{str(e)}（不影响主流程）{COLOR_RESET}")
        logging.warning(f"模板备份失败：{str(e)}")

    categories = []
    current_category = None
    all_channels = []

    try:
        with open(template_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line or (line.startswith("#") and CATEGORY_MARKER not in line):
                    continue

                # 处理分类行
                if CATEGORY_MARKER in line:
                    parts = [p.strip() for p in line.split(CATEGORY_MARKER) if p.strip()]
                    cat_name = parts[0] if parts else ""
                    if not cat_name:
                        safe_print(f"{COLOR_YELLOW}⚠️ 第{line_num}行：分类名为空，忽略{COLOR_RESET}")
                        current_category = None
                        continue
                    existing_cat = next((c for c in categories if c["name"] == cat_name), None)
                    if existing_cat:
                        current_category = cat_name
                    else:
                        categories.append({"name": cat_name, "channels": []})
                        current_category = cat_name
                    continue

                # 处理频道行
                if current_category is None:
                    safe_print(f"{COLOR_YELLOW}⚠️ 第{line_num}行：频道未分类，归入「{UNCATEGORIZED_NAME}」{COLOR_RESET}")
                    if not any(c["name"] == UNCATEGORIZED_NAME for c in categories):
                        categories.append({"name": UNCATEGORIZED_NAME, "channels": []})
                    current_category = UNCATEGORIZED_NAME

                channel_name = clean_text(line.split(",")[0])
                if not channel_name:
                    safe_print(f"{COLOR_YELLOW}⚠️ 第{line_num}行：频道名为空，忽略{COLOR_RESET}")
                    continue
                current_cat_channels = next(c["channels"] for c in categories if c["name"] == current_category)
                if channel_name not in current_cat_channels:
                    current_cat_channels.append(channel_name)
                    if channel_name not in all_channels:
                        all_channels.append(channel_name)

    except Exception as e:
        safe_print(f"{COLOR_RED}❌ 读取模板失败：{str(e)}{COLOR_RESET}")
        logging.error(f"模板读取失败：{str(e)}")
        return None, None

    # 确保"其他频道"存在
    if not any(c["name"] == UNCATEGORIZED_NAME for c in categories):
        categories.append({"name": UNCATEGORIZED_NAME, "channels": []})

    # 输出统计
    total_channels = sum(len(c["channels"]) for c in categories)
    safe_print(f"{COLOR_GREEN}✅ 模板读取完成 | 分类数：{len(categories)} | 总频道数：{total_channels}{COLOR_RESET}")
    safe_print("  " + "-" * 60)
    for idx, cat in enumerate(categories, 1):
        safe_print(f"  {idx:2d}. {cat['name']:<20} 频道数：{len(cat['channels']):2d}")
    safe_print("  " + "-" * 60)
    return categories, all_channels
# =========================================================================

# ======================== 缓存处理函数 =========================
def load_valid_cache() -> Dict[str, Any]:
    """加载缓存并清理过期/超量项（线程安全）"""
    with CACHE_LOCK:
        cache_file = get_cache_file_path()
        if not os.path.exists(cache_file):
            logging.info("缓存文件不存在，返回空缓存")
            return {}
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                cache = json.load(f)
            # 过滤过期缓存
            current_time = time.time()
            valid_cache = {
                url: info for url, info in cache.items()
                if current_time - info.get("timestamp", 0) < CACHE_EXPIRE
            }
            # 清理超量缓存
            if len(valid_cache) > MAX_CACHE_SIZE:
                sorted_cache = sorted(valid_cache.items(), key=lambda x: x[1].get("timestamp", 0), reverse=True)
                valid_cache = dict(sorted_cache[:MAX_CACHE_SIZE])
                safe_print(f"{COLOR_YELLOW}⚠️ 缓存超量，保留最新{MAX_CACHE_SIZE}个{COLOR_RESET}")
            logging.info(f"缓存加载完成：{len(valid_cache)}个有效项")
            return valid_cache
        except Exception as e:
            safe_print(f"{COLOR_YELLOW}⚠️ 加载缓存失败：{str(e)}，使用空缓存{COLOR_RESET}")
            logging.error(f"缓存加载失败：{str(e)}")
            return {}


def save_valid_cache(cache: Dict[str, Any]) -> bool:
    """保存缓存并控制大小（线程安全）"""
    with CACHE_LOCK:
        if len(cache) > MAX_CACHE_SIZE:
            sorted_cache = sorted(cache.items(), key=lambda x: x[1].get("timestamp", 0), reverse=True)
            cache = dict(sorted_cache[:MAX_CACHE_SIZE])
        try:
            cache_file = get_cache_file_path()
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache, f, ensure_ascii=False, indent=2)
            set_file_permissions(cache_file)
            logging.info(f"缓存保存完成：{len(cache)}个项")
            return True
        except Exception as e:
            safe_print(f"{COLOR_YELLOW}⚠️ 保存缓存失败：{str(e)}（不影响主流程）{COLOR_RESET}")
            logging.error(f"缓存保存失败：{str(e)}")
            return False
# =========================================================================

# ======================== 源抓取与测速函数 =========================
def fetch_single(url: str, cache: Dict[str, Any]):
    """抓取单个源内容（结合缓存优化，线程安全）"""
    current_time = time.time()
    # 检查缓存
    with CACHE_LOCK:
        if url in cache:
            cache_info = cache[url]
            if current_time - cache_info.get("timestamp", 0) < CACHE_EXPIRE:
                if cache_info.get("valid", False):
                    safe_print(f"{COLOR_BLUE}🔍 缓存命中[有效]：{url[:50]}{'...' if len(url)>50 else ''}{COLOR_RESET}")
                    return cache_info.get("content", "")
                else:
                    safe_print(f"{COLOR_YELLOW}🔍 缓存命中[无效]：{url[:50]}{'...' if len(url)>50 else ''}（跳过）{COLOR_RESET}")
                    return None

    # 执行抓取
    safe_print(f"{COLOR_BLUE}🔍 开始抓取：{url[:50]}{'...' if len(url)>50 else ''}{COLOR_RESET}")
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0.0.0 Safari/537.36",
            "Accept": "*/*",
            "Connection": "keep-alive"
        }
        # 适配多系统超时
        connect_timeout = 5 if not IS_WINDOWS else 8
        read_timeout = 10 if not IS_WINDOWS else 15
        
        resp = requests.get(
            url, 
            headers=headers, 
            timeout=(connect_timeout, read_timeout),
            allow_redirects=True,
            stream=False
        )
        
        if resp.status_code == 200:
            content = resp.text.strip()
            if len(content) >= MIN_CONTENT_LEN:
                # 更新缓存
                with CACHE_LOCK:
                    cache[url] = {
                        "content": content,
                        "timestamp": current_time,
                        "valid": True
                    }
                safe_print(f"{COLOR_GREEN}✅ 抓取成功：{url[:50]}{'...' if len(url)>50 else ''}{COLOR_RESET}")
                return content
            else:
                safe_print(f"{COLOR_YELLOW}⚠️ 内容过短：{url[:50]}{'...' if len(url)>50 else ''}（{len(content)}字符）{COLOR_RESET}")
        else:
            safe_print(f"{COLOR_YELLOW}⚠️ HTTP错误 {resp.status_code}：{url[:50]}{'...' if len(url)>50 else ''}{COLOR_RESET}")
            
    except Exception as e:
        safe_print(f"{COLOR_RED}❌ 抓取失败：{url[:50]}{'...' if len(url)>50 else ''} - {str(e)}{COLOR_RESET}")
        logging.error(f"抓取失败 {url}: {str(e)}")
    
    # 记录无效结果到缓存
    with CACHE_LOCK:
        cache[url] = {
            "content": "",
            "timestamp": current_time,
            "valid": False
        }
    return None


def speed_test_single(channel_data):
    """单频道测速（返回延迟和速度）"""
    name, url = channel_data
    if not is_valid_url(url):
        return name, url, float('inf'), 0.0
    
    try:
        start_time = time.time()
        resp = requests.get(
            url, 
            timeout=SPEED_TEST_TIMEOUT,
            headers={"User-Agent": "Mozilla/5.0"},
            stream=True
        )
        if resp.status_code == 200:
            # 读取前10KB计算速度
            content = b""
            for chunk in resp.iter_content(chunk_size=1024):
                content += chunk
                if len(content) >= 10240:  # 10KB
                    break
            elapsed = time.time() - start_time
            speed = len(content) / elapsed / 1024  # KB/s
            return name, url, elapsed, speed
    except Exception as e:
        logging.debug(f"测速失败 {name}: {str(e)}")
    
    return name, url, float('inf'), 0.0


def fetch_all_sources():
    """并发抓取所有源"""
    cache = load_valid_cache()
    sources_content = []
    
    with ThreadPoolExecutor(max_workers=MAX_FETCH_WORKERS) as executor:
        future_to_url = {executor.submit(fetch_single, url, cache): url for url in SOURCE_URLS}
        
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                content = future.result()
                if content:
                    sources_content.append(content)
            except Exception as e:
                safe_print(f"{COLOR_RED}❌ 抓取异常：{url} - {str(e)}{COLOR_RESET}")
            
            # 请求间隔
            time.sleep(get_random_interval())
    
    # 保存缓存
    save_valid_cache(cache)
    return sources_content


def parse_channels_from_content(content: str):
    """从源内容解析频道列表"""
    channels = []
    for line in content.splitlines():
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        
        match = CHANNEL_PAT.match(line)
        if match:
            name, url = match.groups()
            name = clean_text(name)
            url = clean_text(url)
            if name and url and is_valid_url(url):
                channels.append((name, url))
    
    return channels


def speed_test_channels(channels, max_workers: int = MAX_SPEED_TEST_WORKERS):
    """并发测速频道列表"""
    safe_print(f"{COLOR_BLUE}🚀 开始测速（{len(channels)}个频道，{max_workers}线程）...{COLOR_RESET}")
    
    valid_channels = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_channel = {executor.submit(speed_test_single, channel): channel for channel in channels}
        
        for future in as_completed(future_to_channel):
            name, url, delay, speed = future.result()
            if delay < float('inf'):
                valid_channels.append((name, url, delay, speed))
                safe_print(f"{COLOR_GREEN}✅ {name:<15} | 延迟: {delay:.2f}s | 速度: {speed:.1f} KB/s{COLOR_RESET}")
            else:
                safe_print(f"{COLOR_RED}❌ {name:<15} | 测速失败{COLOR_RESET}")
    
    # 按延迟排序
    valid_channels.sort(key=lambda x: x[2])
    safe_print(f"{COLOR_GREEN}✅ 测速完成 | 有效频道: {len(valid_channels)}/{len(channels)}{COLOR_RESET}")
    return valid_channels
# =========================================================================

# ======================== 频道分类与输出函数 =========================
def categorize_channels(valid_channels, template_categories):
    """根据模板分类频道"""
    categorized = {cat["name"]: [] for cat in template_categories}
    
    for name, url, delay, speed in valid_channels:
        matched = False
        for category in template_categories:
            if any(template_channel in name for template_channel in category["channels"]):
                categorized[category["name"]].append((name, url, delay, speed))
                matched = True
                break
        
        if not matched:
            categorized[UNCATEGORIZED_NAME].append((name, url, delay, speed))
    
    return categorized


def limit_interfaces_per_channel(categorized_channels):
    """限制单频道接口数量"""
    limited_channels = {}
    
    for category, channels in categorized_channels.items():
        # 按频道名分组
        channel_groups = {}
        for name, url, delay, speed in channels:
            if name not in channel_groups:
                channel_groups[name] = []
            channel_groups[name].append((url, delay, speed))
        
        # 每个频道保留最佳接口
        limited_list = []
        for name, interfaces in channel_groups.items():
            # 按延迟排序，取前N个
            interfaces.sort(key=lambda x: x[1])
            best_interfaces = interfaces[:MAX_INTERFACES_PER_CHANNEL]
            for url, delay, speed in best_interfaces:
                limited_list.append((name, url, delay, speed))
        
        limited_channels[category] = limited_list
    
    return limited_channels


def generate_txt_output(categorized_channels, output_file: str = TXT_OUTPUT) -> bool:
    """生成TXT格式输出"""
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(f"# IPTV频道列表（生成时间：{time.strftime('%Y-%m-%d %H:%M:%S')}）\n")
            f.write(f"# 总频道数：{sum(len(channels) for channels in categorized_channels.values())}\n")
            f.write(f"# 分类数：{len(categorized_channels)}\n\n")
            
            for category, channels in categorized_channels.items():
                if channels:
                    f.write(f"{category},{CATEGORY_MARKER}\n")
                    for name, url, delay, speed in channels:
                        f.write(f"{name},{url}\n")
                    f.write("\n")
        
        set_file_permissions(output_file)
        safe_print(f"{COLOR_GREEN}✅ TXT文件生成成功：{os.path.abspath(output_file)}{COLOR_RESET}")
        return True
    except Exception as e:
        safe_print(f"{COLOR_RED}❌ 生成TXT文件失败：{str(e)}{COLOR_RESET}")
        logging.error(f"TXT输出失败：{str(e)}")
        return False


def generate_m3u_output(categorized_channels, output_file: str = M3U_OUTPUT) -> bool:
    """生成M3U格式输出"""
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write("#EXTM3U\n")
            f.write(f"# Generated by IPTV Tool at {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            
            channel_id = 1
            for category, channels in categorized_channels.items():
                for name, url, delay, speed in channels:
                    f.write(f"#EXTINF:-1 group-title=\"{category}\",{name}\n")
                    f.write(f"{url}\n")
                    channel_id += 1
        
        set_file_permissions(output_file)
        safe_print(f"{COLOR_GREEN}✅ M3U文件生成成功：{os.path.abspath(output_file)}{COLOR_RESET}")
        return True
    except Exception as e:
        safe_print(f"{COLOR_RED}❌ 生成M3U文件失败：{str(e)}{COLOR_RESET}")
        logging.error(f"M3U输出失败：{str(e)}")
        return False


def print_statistics(categorized_channels) -> None:
    """打印统计信息"""
    print_sep("📊 生成统计")
    total_channels = sum(len(channels) for channels in categorized_channels.values())
    
    safe_print(f"{COLOR_BLUE}📺 频道分布：{COLOR_RESET}")
    for category, channels in categorized_channels.items():
        if channels:
            safe_print(f"  {COLOR_GREEN}├─ {category:<15}：{len(channels):>3} 个频道{COLOR_RESET}")
    
    safe_print(f"{COLOR_BLUE}📈 汇总信息：{COLOR_RESET}")
    safe_print(f"  {COLOR_GREEN}├─ 总频道数：{total_channels}{COLOR_RESET}")
    safe_print(f"  {COLOR_GREEN}├─ 分类数量：{len([c for c in categorized_channels.values() if c])}{COLOR_RESET}")
    safe_print(f"  {COLOR_GREEN}└─ 输出文件：{TXT_OUTPUT}, {M3U_OUTPUT}{COLOR_RESET}")
# =========================================================================

# ======================== 主程序入口 =========================
def main():
    """主程序入口"""
    print_sep("🎬 IPTV源处理工具启动")
    
    # 0. 配置验证
    if not validate_config():
        return
    
    # 1. 环境检查
    if not check_network():
        safe_print(f"{COLOR_RED}❌ 网络连接异常，程序退出{COLOR_RESET}")
        return
    
    # 2. 读取模板
    template_categories, all_template_channels = read_template()
    if template_categories is None:
        safe_print(f"{COLOR_RED}❌ 模板处理失败，程序退出{COLOR_RESET}")
        return
    
    # 3. 抓取源数据
    print_sep("🌐 抓取源数据")
    sources_content = fetch_all_sources()
    if not sources_content:
        safe_print(f"{COLOR_RED}❌ 未获取到有效源数据，程序退出{COLOR_RESET}")
        return
    
    # 4. 解析频道
    print_sep("📋 解析频道")
    all_channels = []
    for content in sources_content:
        channels = parse_channels_from_content(content)
        all_channels.extend(channels)
    
    safe_print(f"{COLOR_GREEN}✅ 解析完成 | 原始频道数：{len(all_channels)}{COLOR_RESET}")
    
    if not all_channels:
        safe_print(f"{COLOR_RED}❌ 未解析到有效频道，程序退出{COLOR_RESET}")
        return
    
    # 5. 测速筛选
    print_sep("⚡ 频道测速")
    valid_channels = speed_test_channels(all_channels)
    
    if not valid_channels:
        safe_print(f"{COLOR_RED}❌ 无有效频道通过测速，程序退出{COLOR_RESET}")
        return
    
    # 6. 分类处理
    print_sep("📂 频道分类")
    categorized_channels = categorize_channels(valid_channels, template_categories)
    limited_channels = limit_interfaces_per_channel(categorized_channels)
    
    # 7. 输出文件
    print_sep("💾 生成输出")
    txt_success = generate_txt_output(limited_channels)
    m3u_success = generate_m3u_output(limited_channels)
    
    # 8. 统计信息
    if txt_success or m3u_success:
        print_statistics(limited_channels)
        safe_print(f"{COLOR_GREEN}🎉 IPTV处理完成！{COLOR_RESET}")
    else:
        safe_print(f"{COLOR_RED}❌ 输出文件生成失败{COLOR_RESET}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        safe_print(f"{COLOR_YELLOW}⚠️ 用户中断程序执行{COLOR_RESET}")
    except Exception as e:
        safe_print(f"{COLOR_RED}💥 程序异常：{str(e)}{COLOR_RESET}")
        logging.critical(f"主程序异常：{str(e)}", exc_info=True)
