import requests
import pandas as pd
import re
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# ======================== 核心配置区（已优化）========================
SOURCE_URLS = [
    "https://raw.githubusercontent.com/zwc456baby/iptv_alive/master/live.txt",
    "https://live.zbds.top/tv/iptv6.txt",
    "https://live.zbds.top/tv/iptv4.txt",
]
CATEGORY_TEMPLATE_PATH = "demo.txt"  # 分类模板文件（需与脚本同目录）
MAX_INTERFACES_PER_CHANNEL = 5  # 单频道保留最多接口数
SPEED_TEST_TIMEOUT = 8  # 测速超时时间（秒）
MAX_SPEED_TEST_WORKERS = 15  # 测速并发线程数
OUTPUT_FILE_PREFIX = "iptv.txt"  # 输出文件前缀
CATEGORY_MARKER_RULE = r'^(.+?),(.+)$'  # 分类标识规则：频道分类,#genre#（如“央视频道,综合类”）
# =========================================================================

# 正则表达式（支持HTTP/HTTPS协议的IP匹配）
IPV4_PATTERN = re.compile(r'^https?://(\d{1,3}\.){3}\d{1,3}')
IPV6_PATTERN = re.compile(r'^https?://\[([a-fA-F0-9:]+)\]')
URL_PATTERN = re.compile(r'^https?://')
SPACE_CLEAN_PATTERN = re.compile(r'\s+')
CATEGORY_PATTERN = re.compile(CATEGORY_MARKER_RULE)  # 匹配“分类名,#genre#”格式的分类行


def print_separator(title: str = "", length: int = 70) -> None:
    """打印分隔线，优化日志可读性"""
    if title:
        print(f"\n{'=' * length}")
        print(f"📌 {title}")
        print(f"{'=' * length}")
    else:
        print(f"{'=' * length}")


def clean_text(text: str) -> str:
    """清理文本：去除多余空格、换行符，统一格式"""
    return SPACE_CLEAN_PATTERN.sub("", str(text).strip())


def read_category_template(template_path: str) -> tuple[list[dict], list[dict]] | tuple[None, None]:
    """读取分类模板（分类行：频道分类,#genre#；频道行：频道名,#genre#），返回(分类结构, 频道信息)"""
    if not os.path.exists(template_path):
        print(f"❌ 错误：模板文件「{template_path}」不存在！")
        print(f"📝 模板格式示例：\n  央视频道,综合类\n  CCTV1,综合\n  CCTV2,财经\n  卫视频道,综艺类\n  湖南卫视,综艺")
        return None, None

    categories = []  # 分类结构：[{category: "央视频道", cat_genre: "综合类", channels: [...]}, ...]
    current_category = None
    current_cat_genre = None
    all_channel_info = []  # 频道信息：[{name: "CCTV1", genre: "综合", cat_name: "央视频道", ...}, ...]

    try:
        with open(template_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                # 跳过注释行（#开头且不匹配分类规则）
                if line.startswith("#") and not CATEGORY_PATTERN.match(line):
                    continue

                # 处理分类行（格式：频道分类,#genre#）
                cat_match = CATEGORY_PATTERN.match(line)
                if cat_match:
                    cat_name = clean_text(cat_match.group(1))
                    cat_genre = clean_text(cat_match.group(2)) if len(cat_match.groups()) >= 2 else "未分类"
                    if not cat_name:
                        print(f"⚠️ 第{line_num}行：分类名为空，忽略")
                        current_category = None
                        current_cat_genre = None
                        continue
                    # 合并重复分类
                    existing_cat = next((c for c in categories if c["category"] == cat_name), None)
                    if existing_cat:
                        current_category = cat_name
                        current_cat_genre = existing_cat["cat_genre"]
                    else:
                        categories.append({
                            "category": cat_name,
                            "cat_genre": cat_genre,
                            "channels": []
                        })
                        current_category = cat_name
                        current_cat_genre = cat_genre
                    continue

                # 处理频道行（格式：频道名,#genre#）
                if current_category is None:
                    print(f"⚠️ 第{line_num}行：频道「{line}」未指定分类，归入「未分类」")
                    if not any(c["category"] == "未分类" for c in categories):
                        categories.append({
                            "category": "未分类",
                            "cat_genre": "未分类",
                            "channels": []
                        })
                    current_category = "未分类"
                    current_cat_genre = "未分类"

                # 分割频道名和类型
                ch_parts = line.split(",")
                ch_name = clean_text(ch_parts[0])
                ch_genre = clean_text(ch_parts[1]) if len(ch_parts) >= 2 else "未分类"

                if not ch_name:
                    print(f"⚠️ 第{line_num}行：频道名为空，忽略")
                    continue

                # 记录频道完整信息（含所属分类）
                ch_full_info = {
                    "name": ch_name,
                    "genre": ch_genre,
                    "cat_name": current_category,
                    "cat_genre": current_cat_genre
                }
                # 频道信息去重
                if not any(ch["name"] == ch_name for ch in all_channel_info):
                    all_channel_info.append(ch_full_info)

                # 将频道添加到对应分类
                for cat in categories:
                    if cat["category"] == current_category:
                        if not any(ch["name"] == ch_name for ch in cat["channels"]):
                            cat["channels"].append({"name": ch_name, "genre": ch_genre})
                        break
    except Exception as e:
        print(f"❌ 读取模板失败：{str(e)}")
        return None, None

    if not categories:
        print("⚠️ 警告：模板中未找到有效分类和频道")
        return None, None

    # 打印模板读取结果（简化版）
    total_ch = sum(len(cat["channels"]) for cat in categories)
    print(f"✅ 模板「{template_path}」读取完成 | 分类数：{len(categories)} | 总频道数：{total_ch}")
    print("  " + "-" * 70)
    for idx, cat in enumerate(categories, 1):
        print(f"  {idx:2d}. 分类：{cat['category']:<20} 分类类型：{cat['cat_genre']:<10} 频道数：{len(cat['channels']):2d}")
        for ch in cat["channels"][:3]:
            print(f"       - 频道：{ch['name']:<10} 类型：{ch['genre']}")
        if len(cat["channels"]) > 3:
            print(f"       - ... 等共{len(cat['channels'])}个频道")
    print("  " + "-" * 70)
    return categories, all_channel_info


def fetch_single_source(url: str) -> str | None:
    """抓取单个URL的直播源内容，处理编码和请求错误"""
    print(f"\n🔍 正在抓取：{url}")
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0.0.0 Safari/537.36"
        }
        resp = requests.get(url, timeout=10, headers=headers, allow_redirects=True)
        resp.raise_for_status()  # 抛出HTTP错误（404/500等）
        # 自动处理编码（解决中文乱码）
        if not resp.encoding or resp.encoding.lower() == "iso-8859-1":
            resp.encoding = resp.apparent_encoding
        print(f"✅ 抓取成功 | 内容长度：{len(resp.text):,} 字符")
        return resp.text
    except requests.exceptions.Timeout:
        print(f"❌ 抓取失败：请求超时（超过10秒）")
    except requests.exceptions.ConnectionError:
        print(f"❌ 抓取失败：网络连接错误（无法访问该URL）")
    except requests.exceptions.HTTPError as e:
        print(f"❌ 抓取失败：HTTP错误 {e.response.status_code}")
    except Exception as e:
        print(f"❌ 抓取失败：未知错误 - {str(e)[:50]}")
    return None


def batch_fetch_sources(url_list: list) -> str:
    """批量抓取多个URL的直播源，合并结果"""
    if not url_list:
        print("⚠️ 警告：直播源URL列表为空，无法抓取")
        return ""

    total_url = len(url_list)
    success_count = 0
    combined_content = []
    print(f"📥 开始批量抓取 | 总URL数量：{total_url}")
    print("-" * 70)

    for url in url_list:
        content = fetch_single_source(url)
        if content:
            combined_content.append(content)
            success_count += 1
        else:
            print(f"⏭️  跳过无效URL：{url}")
        print("-" * 70)

    print(f"\n📊 批量抓取统计 | 成功：{success_count} 个 | 失败：{total_url - success_count} 个")
    return "\n".join(combined_content)


def parse_m3u(content: str) -> list[dict]:
    """解析M3U格式直播源，提取频道名和播放地址"""
    if not content.strip():
        print("⚠️ 警告：M3U格式内容为空，无法解析")
        return []

    stream_list = []
    current_program = None
    line_count = 0

    for line in content.splitlines():
        line_count += 1
        line = line.strip()
        # 解析频道名（从#EXTINF行提取tvg-name）
        if line.startswith("#EXTINF"):
            name_match = re.search(r'tvg-name=(["\']?)([^"\']+)\1', line)
            if name_match:
                current_program = clean_text(name_match.group(2))
            continue
        # 解析播放地址（URL行）
        if URL_PATTERN.match(line) and current_program:
            stream_list.append({
                "program_name": current_program,
                "stream_url": line
            })
            current_program = None  # 重置，避免重复匹配

    print(f"📊 M3U解析完成 | 总行数：{line_count:,} | 提取有效流：{len(stream_list)} 个")
    return stream_list


def parse_txt(content: str) -> list[dict]:
    """解析TXT格式直播源（格式：频道名,播放地址）"""
    if not content.strip():
        print("⚠️ 警告：TXT格式内容为空，无法解析")
        return []

    stream_list = []
    line_count = 0
    valid_line_count = 0

    for line in content.splitlines():
        line_count += 1
        line = line.strip()
        # 跳过空行和注释行
        if not line or line.startswith("#"):
            continue
        # 匹配"频道名,URL"格式
        line_match = re.match(r'(.+?)\s*,\s*(https?://.+)$', line)
        if line_match:
            prog_name = clean_text(line_match.group(1))
            stream_url = line_match.group(2).strip()
            if prog_name and stream_url:
                stream_list.append({
                    "program_name": prog_name,
                    "stream_url": stream_url
                })
                valid_line_count += 1
        else:
            print(f"⚠️ 第{line_count}行：格式无效（需为「频道名,URL」），忽略")

    print(f"📊 TXT解析完成 | 总行数：{line_count:,} | 有效行：{valid_line_count} | 提取有效流：{len(stream_list)} 个")
    return stream_list


def test_stream_latency(stream_url: str, timeout: int) -> int | None:
    """测试直播源延迟（毫秒），优先用HEAD请求，细化错误提示"""
    start_time = time.time()
    try:
        # 优先用HEAD请求（轻量，仅获取响应头）
        resp = requests.head(stream_url, timeout=timeout, allow_redirects=True)
        if resp.status_code in [200, 206]:
            return int((time.time() - start_time) * 1000)
        # HEAD请求失败时，用GET请求（仅读取1字节验证可用性）
        resp = requests.get(stream_url, timeout=timeout, allow_redirects=True, stream=True)
        if resp.status_code in [200, 206]:
            resp.iter_content(1).__next__()  # 读取1字节
            return int((time.time() - start_time) * 1000)
    except requests.exceptions.Timeout:
        print(f"⚠️ 测速超时：{stream_url[:50]}{'...' if len(stream_url) > 50 else ''}")
    except requests.exceptions.ConnectionError:
        print(f"⚠️ 测速失败：{stream_url[:50]}{'...' if len(stream_url) > 50 else ''}（网络不可达）")
    except Exception as e:
        print(f"⚠️ 测速错误：{stream_url[:50]}{'...' if len(stream_url) > 50 else ''}（{str(e)[:30]}）")
    return None


def batch_test_latency(stream_df: pd.DataFrame, max_workers: int, timeout: int) -> pd.DataFrame:
    """批量测试直播源延迟，返回按延迟升序排序的有效源DataFrame"""
    if stream_df.empty:
        print("⚠️ 警告：无直播源可测试延迟")
        return pd.DataFrame(columns=["program_name", "stream_url", "latency_ms"])

    total_stream = len(stream_df)
    valid_results = []
    print(f"⚡ 开始批量测速 | 总流数量：{total_stream} | 并发线程：{max_workers} | 超时：{timeout}秒")
    print("-" * 100)

    # 线程池并发测速（提升效率）
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_tasks = {
            executor.submit(test_stream_latency, row["stream_url"], timeout): (row["program_name"], row["stream_url"])
            for _, row in stream_df.iterrows()
        }

        # 处理完成的任务
        for task_idx, future in enumerate(as_completed(future_tasks), 1):
            prog_name, stream_url = future_tasks[future]
            latency = future.result()
            display_url = stream_url[:70] + "..." if len(stream_url) > 70 else stream_url

            if latency is not None:
                valid_results.append({
                    "program_name": prog_name,
                    "stream_url": stream_url,
                    "latency_ms": latency
                })
                print(f"✅ [{task_idx:3d}/{total_stream}] 频道：{prog_name:<20} URL：{display_url:<75} 延迟：{latency:4d}ms")
            else:
                print(f"❌ [{task_idx:3d}/{total_stream}] 频道：{prog_name:<20} URL：{display_url:<75} 状态：无效")

    # 转换为DataFrame并排序
    latency_df = pd.DataFrame(valid_results)
    if not latency_df.empty:
        latency_df = latency_df.sort_values("latency_ms").reset_index(drop=True)

    # 补全截断的打印语句
    print("-" * 100)
    print(f"🏁 批量测速完成 | 有效流：{len(latency_df)} 个 | 无效流：{total_stream - len(latency_df)} 个")
    if len(latency_df) > 0:
        avg_latency = int(latency_df["latency_ms"].mean())
        print(f"📊 延迟统计 | 最快：{latency_df['latency_ms'].min()}ms | 最慢：{latency_df['latency_ms'].max()}ms | 平均：{avg_latency}ms")
    return latency_df


def organize_streams(content: str, categories: list[dict], all_channel_info: list) -> list[dict]:
    """按
