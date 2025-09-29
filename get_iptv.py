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
SPACE_CLEAN_PAT = re.compile(r'^\s+|\s+$|\s+(?=\s)')  # 优化的空格清理正则

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
        safe_print(f"{COLOR_RED}❌ 抓取失败：{msg}{COLOR_RESET}")
        with CACHE_LOCK:
            cache[url] = {"valid": False, "timestamp": current_time}
        return None
    except requests.exceptions.ReadTimeout:
        msg = f"读取超时（>{read_timeout}秒）"
        safe_print(f"{COLOR_RED}❌ 抓取失败：{msg}{COLOR_RESET}")
        with CACHE_LOCK:
            cache[url] = {"valid": False, "timestamp": current_time}
        return None
    except requests.exceptions.TooManyRedirects:
        msg = f"重定向超{MAX_REDIRECTS}次"
        safe_print(f"{COLOR_RED}❌ 抓取失败：{msg}{COLOR_RESET}")
        with CACHE_LOCK:
            cache[url] = {"valid": False, "timestamp": current_time}
        return None
    except requests.exceptions.ConnectionError:
        msg = "网络连接失败"
        safe_print(f"{COLOR_RED}❌ 抓取失败：{msg}{COLOR_RESET}")
        with CACHE_LOCK:
            cache[url] = {"valid": False, "timestamp": current_time}
        return None
    except requests.exceptions.HTTPError as e:
        msg = f"HTTP错误：状态码 {e.response.status_code}"
        safe_print(f"{COLOR_RED}❌ 抓取失败：{msg}{COLOR_RESET}")
        with CACHE_LOCK:
            cache[url] = {"valid": False, "timestamp": current_time}
        return None
    except Exception as e:
        msg = f"未知错误：{str(e)}"
        safe_print(f"{COLOR_RED}❌ 抓取失败：{msg}{COLOR_RESET}")
        with CACHE_LOCK:
            cache[url] = {"valid": False, "timestamp": current_time}
        return None


def batch_fetch(url_list: list) -> str:
    """批量抓取直播源，结合缓存优化和进度显示"""
    # 加载缓存
    cache = load_valid_cache()

    # 步骤1：过滤无效URL
    valid_urls = [u for u in url_list if is_valid_url(u)]
    invalid_cnt = len(url_list) - len(valid_urls)
    if invalid_cnt > 0:
        safe_print(f"{COLOR_YELLOW}⚠️ 过滤无效URL：{invalid_cnt} 个（非HTTP/HTTPS格式）{COLOR_RESET}")
        logging.warning(f"过滤无效URL数量：{invalid_cnt}")
    if not valid_urls:
        safe_print(f"{COLOR_RED}❌ 无有效URL可抓取{COLOR_RESET}")
        return ""

    # 步骤2：并发抓取（带随机间隔）
    combined = []
    interval_cycle = cycle(REQ_INTERVAL)
    total = len(valid_urls)
    print_sep("批量抓取配置")
    safe_print(f"总URL：{total} | 并发数：{MAX_FETCH_WORKERS} | 间隔：{min(REQ_INTERVAL)}-{max(REQ_INTERVAL)}秒 | 缓存项：{len(cache)}")
    print_sep(length=70)

    with ThreadPoolExecutor(max_workers=MAX_FETCH_WORKERS) as executor:
        futures = {}
        for url in valid_urls:
            time.sleep(next(interval_cycle))  # 随机间隔避免反爬
            futures[executor.submit(fetch_single, url, cache)] = url

        # 处理抓取结果（显示进度百分比）
        completed = 0
        for future in as_completed(futures):
            completed += 1
            progress = (completed / total) * 100
            url = futures[future]
            content = future.result()
            if content:
                combined.append(content)
            safe_print(f"{COLOR_YELLOW}📊 抓取进度：{completed}/{total} ({progress:.1f}%){COLOR_RESET}")
            print_sep(length=70)

    # 保存缓存
    save_valid_cache(cache)

    # 输出抓取统计
    success_cnt = len(combined)
    safe_print(f"\n{COLOR_GREEN}📊 抓取统计 | 成功：{success_cnt} 个 | 失败：{total-success_cnt} 个 | 过滤：{invalid_cnt} 个 | 缓存命中：{len(cache)}个{COLOR_RESET}")
    logging.info(f"批量抓取完成 | 总：{total} | 成功：{success_cnt} | 失败：{total-success_cnt} | 缓存更新：{len(cache)}个")
    return "\n".join(combined)


def parse_m3u(content: str) -> list[dict]:
    """解析M3U格式直播源，提取频道名和播放地址"""
    streams = []
    current_ch = None
    line_cnt = 0

    for line in content.splitlines():
        line_cnt += 1
        line = line.strip()
        # 解析频道名（优先tvg-name，其次从描述提取）
        if line.startswith("#EXTINF"):
            tvg_match = re.search(r'tvg-name=(["\']?)([^"\']+)\1', line)
            if tvg_match:
                current_ch = clean_text(tvg_match.group(2))
            else:
                desc_match = re.search(r',([^,]+)$', line)
                if desc_match:
                    current_ch = clean_text(desc_match.group(1))
            continue
        # 解析播放地址
        if URL_PAT.match(line) and current_ch:
            streams.append({"name": current_ch, "url": line})
            current_ch = None  # 重置避免重复匹配

    safe_print(f"{COLOR_GREEN}📊 M3U解析结果 | 总行数：{line_cnt:,} | 提取有效源：{len(streams)} 个{COLOR_RESET}")
    return streams


def parse_txt(content: str) -> list[dict]:
    """解析TXT格式直播源（格式：频道名,URL）"""
    streams = []
    line_cnt = 0
    valid_cnt = 0

    for line in content.splitlines():
        line_cnt += 1
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        # 兼容空格分隔，统一转为逗号分隔
        line = line.replace(" ", ",")
        parts = [p.strip() for p in line.split(",") if p.strip()]
        # 需包含频道名和URL（至少2部分，最后一部分为URL）
        if len(parts) >= 2 and URL_PAT.match(parts[-1]):
            ch_name = clean_text(",".join(parts[:-1]))
            streams.append({"name": ch_name, "url": parts[-1]})
            valid_cnt += 1
        else:
            safe_print(f"{COLOR_YELLOW}⚠️ 第{line_cnt}行：格式无效（需为「频道名,URL」），忽略{COLOR_RESET}")

    safe_print(f"{COLOR_GREEN}📊 TXT解析结果 | 总行数：{line_cnt:,} | 有效行：{valid_cnt} | 提取有效源：{len(streams)} 个{COLOR_RESET}")
    return streams


def test_latency(url: str) -> int | None:
    """测试单个直播源延迟（毫秒），优先HEAD请求，失败降级为GET"""
    start = time.time()
    try:
        # 先尝试HEAD请求（轻量），失败则用GET请求验证
        for method in [requests.head, requests.get]:
            with method(
                url,
                timeout=SPEED_TEST_TIMEOUT,
                allow_redirects=True,
                stream=(method == requests.get)
            ) as resp:
                if resp.status_code in [200, 206]:
                    if method == requests.get:
                        resp.iter_content(1).__next__()  # 读取1字节确认可用性
                    return int((time.time() - start) * 1000)
    except Exception:
        return None  # 异常直接返回None，不在此打印日志


def batch_test(streams: list[dict]) -> pd.DataFrame:
    """批量测试直播源延迟，返回按延迟排序的DataFrame"""
    if not streams:
        safe_print(f"{COLOR_RED}❌ 无直播源可测速{COLOR_RESET}")
        return pd.DataFrame(columns=["name", "url", "latency"])

    stream_df = pd.DataFrame(streams)
    total = len(stream_df)
    valid = []
    print_sep("批量测速配置")
    safe_print(f"总源数：{total} | 并发数：{MAX_SPEED_TEST_WORKERS} | 超时：{SPEED_TEST_TIMEOUT}秒")
    print_sep(length=100)

    with ThreadPoolExecutor(max_workers=MAX_SPEED_TEST_WORKERS) as executor:
        futures = {
            executor.submit(test_latency, row["url"]): (row["name"], row["url"])
            for _, row in stream_df.iterrows()
        }

        # 处理测速结果（显示进度）
        completed = 0
        for idx, future in enumerate(as_completed(futures), 1):
            completed += 1
            progress = (completed / total) * 100
            ch_name, url = futures[future]
            latency = future.result()
            display_url = url[:70] + "..." if len(url) > 70 else url

            if latency is not None:
                valid.append({"name": ch_name, "url": url, "latency": latency})
                safe_print(f"{COLOR_GREEN}✅ [{idx:3d}/{total} ({progress:.1f}%)] 频道：{ch_name:<20} URL：{display_url:<75} 延迟：{latency:4d}ms{COLOR_RESET}")
            else:
                safe_print(f"{COLOR_RED}❌ [{idx:3d}/{total} ({progress:.1f}%)] 频道：{ch_name:<20} URL：{display_url:<75} 状态：无效{COLOR_RESET}")

    # 转换为DataFrame并按延迟排序
    latency_df = pd.DataFrame(valid)
    if not latency_df.empty:
        latency_df = latency_df.sort_values("latency").reset_index(drop=True)

    # 输出测速统计
    print_sep(length=100)
    safe_print(f"🏁 测速完成 | 有效源：{len(latency_df)} 个 | 无效源：{total - len(latency_df)} 个")
    if len(latency_df) > 0:
        avg_lat = int(latency_df["latency"].mean())
        safe_print(f"📊 延迟统计 | 最快：{latency_df['latency'].min()}ms | 最慢：{latency_df['latency'].max()}ms | 平均：{avg_lat}ms")
    logging.info(f"批量测速完成 | 总：{total} | 有效：{len(latency_df)} | 平均延迟：{avg_lat if len(latency_df) > 0 else 0}ms")
    return latency_df


def organize_streams(raw_content: str, categories: list[dict], all_channels: list) -> list[dict]:
    """按分类模板整理直播源：过滤匹配、测速排序、限制接口数"""
    print_sep("开始整理直播源（4个步骤）")

    # 步骤1：自动识别格式并解析
    if raw_content.startswith("#EXTM3U") or "#EXTINF" in raw_content[:100]:
        safe_print("1. 识别格式：M3U")
        parsed_streams = parse_m3u(raw_content)
    else:
        safe_print("1. 识别格式：TXT（默认）")
        parsed_streams = parse_txt(raw_content)

    if not parsed_streams:
        safe_print(f"{COLOR_RED}❌ 解析后无有效直播源，整理终止{COLOR_RESET}")
        return []

    # 步骤2：按模板频道过滤（仅保留模板中存在的频道）
    safe_print(f"2. 按模板过滤 | 解析源数：{len(parsed_streams)} | 模板频道数：{len(all_channels)}")
    matched_streams = []
    for stream in parsed_streams:
        # 模糊匹配（忽略大小写）
        if any(clean_text(stream["name"]).lower() == clean_text(ch).lower() for ch in all_channels):
            matched_streams.append(stream)

    if not matched_streams:
        safe_print(f"{COLOR_RED}❌ 无直播源匹配模板频道，整理终止{COLOR_RESET}")
        return []
    safe_print(f"   匹配成功：{len(matched_streams)} 个源")

    # 步骤3：批量测速并按延迟排序
    safe_print("3. 开始批量测速（按延迟升序排序）")
    sorted_df = batch_test(matched_streams)
    if sorted_df.empty:
        safe_print(f"{COLOR_RED}❌ 测速后无有效源，整理终止{COLOR_RESET}")
        return []

    # 步骤4：按分类分组并限制单频道接口数
    safe_print("4. 按分类分组并限制接口数")
    organized = []
    for cat in categories:
        cat_streams = []
        for _, row in sorted_df.iterrows():
            # 匹配分类下的频道
            if clean_text(row["name"]).lower() in [clean_text(ch).lower() for ch in cat["channels"]]:
                cat_streams.append({
                    "category": cat["name"],
                    "name": row["name"],
                    "url": row["url"],
                    "latency": row["latency"]
                })
        # 按频道去重并限制每个频道的接口数
        ch_count = {}
        filtered_cat = []
        for s in cat_streams:
            ch_key = clean_text(s["name"]).lower()
            if ch_count.get(ch_key, 0) < MAX_INTERFACES_PER_CHANNEL:
                filtered_cat.append(s)
                ch_count[ch_key] = ch_count.get(ch_key, 0) + 1
        organized.extend(filtered_cat)

    safe_print(f"{COLOR_GREEN}✅ 整理完成 | 最终有效源数：{len(organized)}{COLOR_RESET}")
    return organized


def save_results(organized_streams: list[dict]) -> bool:
    """保存整理后的直播源到iptv.txt和iptv.m3u"""
    if not organized_streams:
        safe_print(f"{COLOR_RED}❌ 无有效源可保存{COLOR_RESET}")
        return False

    # 1. 保存TXT文件（按分类分组）
    try:
        with open(TXT_OUTPUT, 'w', encoding='utf-8') as f:
            f.write(f"# IPTV直播源列表（生成时间：{time.strftime('%Y-%m-%d %H:%M:%S')}）\n")
            f.write(f"# 系统：{SYSTEM} | 总源数：{len(organized_streams)} | 单频道最大接口数：{MAX_INTERFACES_PER_CHANNEL}\n\n")
            
            current_cat = None
            for s in organized_streams:
                if s["category"] != current_cat:
                    current_cat = s["category"]
                    f.write(f"# {CATEGORY_MARKER} {current_cat}\n")
                f.write(f"{s['name']},{s['url']},延迟：{s['latency']}ms\n")
        set_file_permissions(TXT_OUTPUT)
        safe_print(f"{COLOR_GREEN}✅ TXT文件保存成功：{os.path.abspath(TXT_OUTPUT)}{COLOR_RESET}")
    except Exception as e:
        safe_print(f"{COLOR_RED}❌ TXT文件保存失败：{str(e)}{COLOR_RESET}")
        logging.error(f"TXT保存失败：{str(e)}")
        return False

    # 2. 保存M3U文件（支持播放器识别）
    try:
        with open(M3U_OUTPUT, 'w', encoding='utf-8') as f:
            f.write("#EXTM3U x-tvg-url=\"http://epg.51zmt.top:8000/e.xml\"\n")
            f.write(f"# 生成时间：{time.strftime('%Y-%m-%d %H:%M:%S')} | 系统：{SYSTEM} | 总源数：{len(organized_streams)}\n")
            
            for s in organized_streams:
                f.write(f"#EXTINF:-1 tvg-name=\"{s['name']}\" group-title=\"{s['category']}\",{s['name']}\n")
                f.write(f"{s['url']}\n")
        set_file_permissions(M3U_OUTPUT)
        safe_print(f"{COLOR_GREEN}✅ M3U文件保存成功：{os.path.abspath(M3U_OUTPUT)}{COLOR_RESET}")
    except Exception as e:
        safe_print(f"{COLOR_RED}❌ M3U文件保存失败：{str(e)}{COLOR_RESET}")
        logging.error(f"M3U保存失败：{str(e)}")
        return False

    logging.info(f"结果保存完成 | TXT：{len(organized_streams)} 个源 | M3U：{len(organized_streams)} 个源")
    return True


if __name__ == "__main__":
    print_sep("IPTV直播源分类整理工具（终极完美版）", length=70)
    start_time = time.time()

    try:
        # 步骤1：检测网络
        if not check_network():
            raise Exception("网络连接异常，无法继续")

        # 步骤2：读取分类模板
        print_sep("步骤1/4：读取分类模板")
        categories, all_channels = read_template()
        if not categories or not all_channels:
            raise Exception("模板读取失败，无法继续")

        # 步骤3：批量抓取直播源
        print_sep("步骤2/4：批量抓取直播源")
        raw_content = batch_fetch(SOURCE_URLS)
        if not raw_content.strip():
            raise Exception("未抓取到有效直播源内容")

        # 步骤4：按模板整理直播源（解析→过滤→测速→分组）
        print_sep("步骤3/4：整理直播源")
        organized_streams = organize_streams(raw_content, categories, all_channels)
        if not organized_streams:
            raise Exception("直播源整理后无有效数据")

        # 步骤5：保存结果到固定文件
        print_sep("步骤4/4：保存结果文件")
        save_success = save_results(organized_streams)
        if not save_success:
            raise Exception("结果文件保存失败")

        # 流程完成
        total_time = round(time.time() - start_time, 2)
        print_sep("工具执行完成", length=70)
        safe_print(f"{COLOR_GREEN}🎉 所有流程成功完成！{COLOR_RESET}")
        safe_print(f"⏱️  总耗时：{total_time} 秒")
        safe_print(f"📁 输出文件：")
        safe_print(f"   - {os.path.abspath(TXT_OUTPUT)}")
        safe_print(f"   - {os.path.abspath(M3U_OUTPUT)}")
        safe_print(f"📝 日志文件：{os.path.abspath('iptv_tool.log')}")

    except Exception as e:
        print_sep("工具执行失败", length=70)
        safe_print(f"{COLOR_RED}❌ 失败原因：{str(e)}{COLOR_RESET}")
        logging.error(f"整体流程失败：{str(e)}")
        exit(1)
