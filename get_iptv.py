import requests
import pandas as pd
import re
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# ======================== 核心配置区（可按需修改）========================
SOURCE_URLS = [
    "https://raw.githubusercontent.com/zwc456baby/iptv_alive/master/live.txt",
    "https://live.zbds.top/tv/iptv6.txt",
    "https://live.zbds.top/tv/iptv4.txt",
]
CATEGORY_TEMPLATE_PATH = "iptv_channels_template.txt"  # 分类模板路径
MAX_INTERFACES_PER_CHANNEL = 5  # 单频道最大接口数
SPEED_TEST_TIMEOUT = 8  # 测速超时（秒）
MAX_SPEED_TEST_WORKERS = 15  # 测速并发数
OUTPUT_FILE_PREFIX = "iptv_organized"  # 输出文件前缀
CATEGORY_MARKER = "##"  # 模板分类标记（如"## 央视频道"）
# =========================================================================

# 正则表达式（内部使用）
IPV4_PATTERN = re.compile(r'^http://(\d{1,3}\.){3}\d{1,3}')
IPV6_PATTERN = re.compile(r'^http://\[([a-fA-F0-9:]+)\]')
URL_PATTERN = re.compile(r'^https?://')
SPACE_CLEAN_PATTERN = re.compile(r'\s+')


def print_separator(title: str = "", length: int = 70) -> None:
    """打印分隔线，优化日志可读性"""
    if title:
        print(f"\n{'=' * length}")
        print(f"📌 {title}")
        print(f"{'=' * length}")
    else:
        print(f"{'=' * length}")


def clean_text(text: str) -> str:
    """清理文本：去除多余空格、换行符"""
    return SPACE_CLEAN_PATTERN.sub("", str(text).strip())


def read_category_template(template_path: str) -> tuple[list[dict], list[str]] | tuple[None, None]:
    """读取分类模板，返回(分类结构, 去重频道列表)或(None, None)"""
    if not os.path.exists(template_path):
        print(f"❌ 错误：模板文件「{template_path}」不存在！")
        print(f"📝 模板格式示例：\n  {CATEGORY_MARKER} 央视频道\n  CCTV1\n  CCTV2\n  {CATEGORY_MARKER} 卫视频道\n  湖南卫视")
        return None, None

    categories = []
    current_category = None
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
                        print(f"⚠️ 第{line_num}行：分类名无效，忽略")
                        current_category = None
                        continue
                    # 合并重复分类
                    existing = next((c for c in categories if c["category"] == cat_name), None)
                    if existing:
                        current_category = cat_name
                    else:
                        categories.append({"category": cat_name, "channels": []})
                        current_category = cat_name
                    continue

                # 处理频道行
                if current_category is None:
                    print(f"⚠️ 第{line_num}行：频道未分类，归入「未分类」")
                    if not any(c["category"] == "未分类" for c in categories):
                        categories.append({"category": "未分类", "channels": []})
                    current_category = "未分类"

                ch_name = clean_text(line.split(",")[0])
                if not ch_name:
                    print(f"⚠️ 第{line_num}行：频道名无效，忽略")
                    continue
                if ch_name not in all_channels:
                    all_channels.append(ch_name)
                    for cat in categories:
                        if cat["category"] == current_category:
                            cat["channels"].append(ch_name)
                            break
    except Exception as e:
        print(f"❌ 读取模板失败：{str(e)}")
        return None, None

    if not categories:
        print("⚠️ 警告：模板无有效分类/频道")
        return None, None

    total_ch = sum(len(cat["channels"]) for cat in categories)
    print(f"✅ 模板读取完成 | 分类数：{len(categories)} | 频道数：{total_ch}")
    print("  " + "-" * 50)
    for idx, cat in enumerate(categories, 1):
        print(f"  {idx:2d}. 分类：{cat['category']:<20} 频道数：{len(cat['channels']):2d}")
    print("  " + "-" * 50)
    return categories, all_channels


def fetch_single_source(url: str) -> str | None:
    """抓取单个URL的直播源内容"""
    print(f"\n🔍 抓取：{url}")
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0.0.0 Safari/537.36"
        }
        resp = requests.get(url, timeout=10, headers=headers, allow_redirects=True)
        resp.raise_for_status()
        if not resp.encoding or resp.encoding.lower() == "iso-8859-1":
            resp.encoding = resp.apparent_encoding
        print(f"✅ 成功 | 长度：{len(resp.text):,} 字符")
        return resp.text
    except requests.exceptions.Timeout:
        print(f"❌ 失败：请求超时")
    except requests.exceptions.ConnectionError:
        print(f"❌ 失败：网络错误")
    except requests.exceptions.HTTPError as e:
        print(f"❌ 失败：HTTP {e.response.status_code}")
    except Exception as e:
        print(f"❌ 失败：{str(e)[:50]}")
    return None


def batch_fetch_sources(url_list: list) -> str:
    """批量抓取多个URL的直播源"""
    if not url_list:
        print("⚠️ 警告：URL列表为空")
        return ""

    total = len(url_list)
    success = 0
    combined = []
    print(f"📥 批量抓取 | 总URL数：{total}")
    print("-" * 70)

    for url in url_list:
        content = fetch_single_source(url)
        if content:
            combined.append(content)
            success += 1
        else:
            print(f"⏭️  跳过无效URL：{url}")
        print("-" * 70)

    print(f"\n📊 抓取统计 | 成功：{success} 个 | 失败：{total - success} 个")
    return "\n".join(combined)


def parse_m3u(content: str) -> list[dict]:
    """解析M3U格式直播源"""
    if not content.strip():
        print("⚠️ 警告：M3U内容为空")
        return []

    streams = []
    current_prog = None
    line_count = 0

    for line in content.splitlines():
        line_count += 1
        line = line.strip()
        if line.startswith("#EXTINF"):
            match = re.search(r'tvg-name=(["\']?)([^"\']+)\1', line)
            if match:
                current_prog = clean_text(match.group(2))
            continue
        if URL_PATTERN.match(line) and current_prog:
            streams.append({"program_name": current_prog, "stream_url": line})
            current_prog = None

    print(f"📊 M3U解析 | 总行数：{line_count:,} | 提取源：{len(streams)} 个")
    return streams


def parse_txt(content: str) -> list[dict]:
    """解析TXT格式直播源（频道名,URL）"""
    if not content.strip():
        print("⚠️ 警告：TXT内容为空")
        return []

    streams = []
    line_count = 0
    valid = 0

    for line in content.splitlines():
        line_count += 1
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        match = re.match(r'(.+?)\s*,\s*(https?://.+)$', line)
        if match:
            prog = clean_text(match.group(1))
            url = match.group(2).strip()
            if prog and url:
                streams.append({"program_name": prog, "stream_url": url})
                valid += 1
        else:
            print(f"⚠️ 第{line_count}行：格式无效，忽略")

    print(f"📊 TXT解析 | 总行数：{line_count:,} | 有效行：{valid} | 提取源：{len(streams)} 个")
    return streams


def test_stream_latency(stream_url: str, timeout: int) -> int | None:
    """测试直播源延迟（毫秒），优先HEAD请求"""
    start = time.time()
    try:
        resp = requests.head(stream_url, timeout=timeout, allow_redirects=True)
        if resp.status_code in [200, 206]:
            return int((time.time() - start) * 1000)
        resp = requests.get(stream_url, timeout=timeout, allow_redirects=True, stream=True)
        if resp.status_code in [200, 206]:
            resp.iter_content(1).__next__()
            return int((time.time() - start) * 1000)
    except Exception:
        pass
    return None


def batch_test_latency(stream_df: pd.DataFrame, max_workers: int, timeout: int) -> pd.DataFrame:
    """批量测试直播源延迟，返回按延迟排序的有效源"""
    if stream_df.empty:
        print("⚠️ 警告：无直播源可测试")
        return pd.DataFrame(columns=["program_name", "stream_url", "latency_ms"])

    total = len(stream_df)
    results = []
    print(f"⚡ 批量测速 | 源数：{total} | 并发：{max_workers} | 超时：{timeout}秒")
    print("-" * 95)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(test_stream_latency, row["stream_url"], timeout): (row["program_name"], row["stream_url"])
            for _, row in stream_df.iterrows()
        }

        for idx, future in enumerate(as_completed(future_map), 1):
            prog, url = future_map[future]
            latency = future.result()
            display_url = url[:65] + "..." if len(url) > 65 else url

            if latency is not None:
                results.append({"program_name": prog, "stream_url": url, "latency_ms": latency})
                print(f"✅ [{idx:3d}/{total}] 频道：{prog:<20} URL：{display_url:<70} 延迟：{latency:4d}ms")
            else:
                print(f"❌ [{idx:3d}/{total}] 频道：{prog:<20} URL：{display_url:<70} 状态：失败")

    latency_df = pd.DataFrame(results)
    if not latency_df.empty:
        latency_df = latency_df.sort_values("latency_ms").reset_index(drop=True)

    print("-" * 95)
    print(f"🏁 测速完成 | 有效：{len(latency_df)} 个 | 无效：{total - len(latency_df)} 个")
    if len(latency_df) > 0:
        avg = latency_df["latency_ms"].mean()
        print(f"📊 统计 | 最快：{latency_df['latency_ms'].min()}ms | 最慢：{latency_df['latency_ms'].max()}ms | 平均：{avg:.0f}ms")
    return latency_df


def organize_streams(content: str, categories: list[dict], all_channels: list) -> list[dict]:
    """按分类整理直播源：解析→过滤→测速→限制接口数"""
    # 步骤1：解析
    if content.startswith("#EXTINF"):
        print("\n🔧 步骤1/4：解析M3U格式...")
        parsed = parse_m3u(content)
    else:
        print("\n🔧 步骤1/4：解析TXT格式...")
        parsed = parse_txt(content)
    stream_df = pd.DataFrame(parsed)
    if stream_df.empty:
        print("❌ 整理失败：无解析结果")
        return []

    # 步骤2：过滤+去重
    print(f"\n🔧 步骤2/4：过滤并去重...")
    stream_df["program_clean"] = stream_df["program_name"].apply(clean_text)
    template_clean = [clean_text(ch) for ch in all_channels]
    filtered_df = stream_df[stream_df["program_clean"].isin(template_clean)].copy()
    filtered_df = filtered_df.drop_duplicates(subset=["program_name", "stream_url"]).reset_index(drop=True)
    if filtered_df.empty:
        print("❌ 整理失败：无匹配模板的频道")
        return []
    print(f"  结果 | 原始：{len(stream_df)} 个 | 匹配：{len(filtered_df)} 个 | 过滤：{len(stream_df)-len(filtered_df)} 个")

    # 步骤3：测速
    print(f"\n🔧 步骤3/4：批量测速...")
    valid_df = batch_test_latency(filtered_df[["program_name", "stream_url"]], MAX_SPEED_TEST_WORKERS, SPEED_TEST_TIMEOUT)
    if valid_df.empty:
        print("❌ 整理失败：所有源测速失败")
        return []

    # 步骤4：分类整理
    print(f"\n🔧 步骤4/4：按分类整理...")
    organized = []
    for cat in categories:
        cat_name = cat["category"]
        cat_ch_clean = [clean_text(ch) for ch in cat["channels"]]
        cat_df = valid_df[valid_df["program_name"].apply(clean_text).isin(cat_ch_clean)].copy()

        if cat_df.empty:
            print(f"⚠️ 分类「{cat_name}」：无有效源，跳过")
            continue

        # 按模板顺序排序
        ch_order = {ch: idx for idx, ch in enumerate(cat["channels"])}
        cat_df["order"] = cat_df["program_name"].apply(
            lambda x: ch_order.get(next((ch for ch in cat["channels"] if clean_text(ch) == clean_text(x)), ""), 999)
        )
        cat_df_sorted = cat_df.sort_values(["order", "latency_ms"]).reset_index(drop=True)

        # 限制单频道接口数
        def limit_ifs(group):
            limited = group.head(MAX_INTERFACES_PER_CHANNEL)
            return pd.Series({
                "stream_urls": limited["stream_url"].tolist(),
                "interface_count": len(limited)
            })
        cat_grouped = cat_df_sorted.groupby("program_name").apply(limit_ifs).reset_index()
        cat_grouped = cat_grouped[cat_grouped["interface_count"] > 0].reset_index(drop=True)

        # 整理分类结果
        cat_result = []
        for _, row in cat_grouped.iterrows():
            cat_result.append({
                "program_name": row["program_name"],
                "interface_count": row["interface_count"],
                "stream_urls": row["stream_urls"]
            })

        organized.append({"category": cat_name, "channels": cat_result})

    if not organized:
        print("❌ 整理失败：无有效分类结果")
        return []

    # 统计结果
    total_cats = len(organized)
    total_chs = sum(len(cat["channels"]) for cat in organized)
    total_ifs = sum(ch["interface_count"] for cat in organized for ch in cat["channels"])
    print(f"\n✅ 整理完成 | 分类：{total_cats} 个 | 频道：{total_chs} 个 | 接口：{total_ifs} 个")
    return organized


def save_organized_results(organized_data: list[dict]) -> None:
    """保存整理结果为TXT和M3U文件"""
    if not organized_data:
        print("⚠️ 无有效数据可保存")
        return

    total_cats = len(organized_data)
    total_chs = sum(len(cat["channels"]) for cat in organized_data)
    total_ifs = sum(ch["interface_count"] for cat in organized_data for ch in cat["channels
