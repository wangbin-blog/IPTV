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
CATEGORY_TEMPLATE_PATH = "demo.txt"  # 分类模板路径
MAX_INTERFACES_PER_CHANNEL = 5  # 单频道最大接口数
SPEED_TEST_TIMEOUT = 8  # 测速超时（秒）
MAX_SPEED_TEST_WORKERS = 15  # 测速并发数
MAX_FETCH_WORKERS = 5  # 抓取并发数（避免请求过载）
OUTPUT_FILE_PREFIX = "iptv"  # 输出文件前缀
CATEGORY_MARKER = "#genre#"  # 分类模板标记
# =========================================================================

# 正则表达式（优化适配 HTTPS）
IPV4_PATTERN = re.compile(r'^https?://(\d{1,3}\.){3}\d{1,3}')
IPV6_PATTERN = re.compile(r'^https?://\[([a-fA-F0-9:]+)\]')
URL_PATTERN = re.compile(r'^https?://')
SPACE_CLEAN_PATTERN = re.compile(r'\s+')


def print_separator(title: str = "", length: int = 70) -> None:
    """打印分隔线，优化可读性"""
    sep = "=" * length
    if title:
        print(f"\n{sep}")
        print(f"📌 {title}")
        print(sep)
    else:
        print(sep)


def clean_text(text: str) -> str:
    """清理文本：去除多余空格、换行符"""
    return SPACE_CLEAN_PATTERN.sub("", str(text).strip())


def read_category_template(template_path: str) -> tuple[list[dict], list[str]] | tuple[None, None]:
    """读取分类模板，返回(分类结构, 去重频道列表)或(None, None)"""
    if not os.path.exists(template_path):
        print(f"模板文件「{template_path}」不存在！")
        print(f"模板格式示例：\n  {CATEGORY_MARKER} 央视频道\n  CCTV1\n  CCTV2\n  {CATEGORY_MARKER} 卫视频道\n  湖南卫视")
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
                # 跳过非分类标记的注释行
                if line.startswith("#") and not line.startswith(CATEGORY_MARKER):
                    continue

                # 处理分类行（匹配#genre#标记）
                if line.startswith(CATEGORY_MARKER):
                    cat_name = clean_text(line.lstrip(CATEGORY_MARKER))
                    if not cat_name:
                        print(f"第{line_num}行：分类名无效，忽略")
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
                    print(f"第{line_num}行：频道未分类，归入「未分类」")
                    if not any(c["category"] == "未分类" for c in categories):
                        categories.append({"category": "未分类", "channels": []})
                    current_category = "未分类"

                ch_name = clean_text(line.split(",")[0])
                if not ch_name:
                    print(f"第{line_num}行：频道名无效，忽略")
                    continue
                # 去重并添加到分类
                if ch_name not in all_channels:
                    all_channels.append(ch_name)
                    for cat in categories:
                        if cat["category"] == current_category:
                            cat["channels"].append(ch_name)
                            break
    except Exception as e:
        print(f"读取模板失败：{str(e)}")
        return None, None

    if not categories:
        print("模板无有效分类/频道")
        return None, None

    # 输出模板统计
    total_ch = sum(len(cat["channels"]) for cat in categories)
    print(f"✅ 模板读取完成 | 分类数：{len(categories)} | 频道数：{total_ch}")
    print("  " + "-" * 50)
    for idx, cat in enumerate(categories, 1):
        print(f"  {idx:2d}. 分类：{cat['category']:<20} 频道数：{len(cat['channels']):2d}")
    print("  " + "-" * 50)
    return categories, all_channels


def fetch_single_source(url: str) -> str | None:
    """抓取单个URL的直播源内容（优化超时和编码处理）"""
    print(f"\n🔍 抓取：{url}")
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0.0.0 Safari/537.36",
            "Accept": "*/*",
            "Connection": "keep-alive"
        }
        resp = requests.get(
            url,
            timeout=(5, SPEED_TEST_TIMEOUT),  # 连接超时5s，读取超时按配置
            headers=headers,
            allow_redirects=True,
            stream=False
        )
        resp.raise_for_status()
        # 智能编码处理（解决中文乱码）
        if not resp.encoding or resp.encoding.lower() == "iso-8859-1":
            resp.encoding = resp.apparent_encoding
        print(f"✅ 成功 | 长度：{len(resp.text):,} 字符")
        return resp.text
    except requests.exceptions.ConnectTimeout:
        print(f"❌ 失败：连接超时（超过5秒）")
    except requests.exceptions.ReadTimeout:
        print(f"❌ 失败：读取超时（超过{SPEED_TEST_TIMEOUT}秒）")
    except requests.exceptions.ConnectionError:
        print(f"❌ 失败：网络错误（无法连接）")
    except requests.exceptions.HTTPError as e:
        print(f"❌ 失败：HTTP {e.response.status_code}")
    except Exception as e:
        print(f"❌ 失败：{str(e)[:50]}")
    return None


def batch_fetch_sources(url_list: list) -> str:
    """批量抓取多个URL的直播源（并发抓取提升效率）"""
    if not url_list:
        print("URL列表为空")
        return ""

    total = len(url_list)
    combined_content = []
    print(f"📥 批量抓取 | 总URL数：{total} | 并发数：{MAX_FETCH_WORKERS}")
    print("-" * 70)

    with ThreadPoolExecutor(max_workers=MAX_FETCH_WORKERS) as executor:
        future_tasks = {executor.submit(fetch_single_source, url): url for url in url_list}
        for future in as_completed(future_tasks):
            url = future_tasks[future]
            content = future.result()
            if content:
                combined_content.append(content)
            else:
                print(f"⏭️  跳过无效URL：{url}")
            print("-" * 70)

    success_count = len(combined_content)
    print(f"\n📊 抓取统计 | 成功：{success_count} 个 | 失败：{total - success_count} 个")
    return "\n".join(combined_content)


def parse_m3u(content: str) -> list[dict]:
    """解析M3U格式直播源，提取频道名和播放地址"""
    if not content.strip():
        print("M3U内容为空")
        return []

    streams = []
    current_program = None
    line_count = 0

    for line in content.splitlines():
        line_count += 1
        line = line.strip()
        # 解析频道名（优先tvg-name，其次从描述提取）
        if line.startswith("#EXTINF"):
            tvg_match = re.search(r'tvg-name=(["\']?)([^"\']+)\1', line)
            if tvg_match:
                current_program = clean_text(tvg_match.group(2))
            else:
                desc_match = re.search(r',([^,]+)$', line)
                if desc_match:
                    current_program = clean_text(desc_match.group(1))
            continue
        # 解析播放地址（URL行）
        if URL_PATTERN.match(line) and current_program:
            streams.append({"program_name": current_program, "stream_url": line})
            current_program = None  # 重置，避免重复匹配

    print(f"📊 M3U解析 | 总行数：{line_count:,} | 提取源：{len(streams)} 个")
    return streams


def parse_txt(content: str) -> list[dict]:
    """解析TXT格式直播源（频道名,URL 格式）"""
    if not content.strip():
        print("TXT内容为空")
        return []

    streams = []
    line_count = 0
    valid_count = 0

    for line in content.splitlines():
        line_count += 1
        line = line.strip()
        # 跳过空行和注释行
        if not line or line.startswith("#"):
            continue
        # 兼容空格分隔，统一转为逗号分隔
        line = line.replace(" ", ",")
        parts = [p.strip() for p in line.split(",") if p.strip()]
        # 需包含频道名和URL（至少2个部分，且最后一个是URL）
        if len(parts) >= 2 and URL_PATTERN.match(parts[-1]):
            program_name = clean_text(",".join(parts[:-1]))  # 支持含逗号的频道名
            stream_url = parts[-1]
            streams.append({"program_name": program_name, "stream_url": stream_url})
            valid_count += 1
        else:
            print(f"第{line_count}行：格式无效（需为「频道名,URL」），忽略")

    print(f"📊 TXT解析 | 总行数：{line_count:,} | 有效行：{valid_count} | 提取源：{len(streams)} 个")
    return streams


def test_stream_latency(stream_url: str, timeout: int) -> int | None:
    """测试直播源延迟（毫秒），优先HEAD请求，失败降级为GET"""
    start_time = time.time()
    try:
        for method in [requests.head, requests.get]:
            with method(
                stream_url,
                timeout=timeout,
                allow_redirects=True,
                stream=(method == requests.get)
            ) as resp:
                if resp.status_code in [200, 206]:
                    if method == requests.get:
                        resp.iter_content(1).__next__()  # 读1字节验证可用性
                    return int((time.time() - start_time) * 1000)
    except requests.exceptions.Timeout:
        print(f"⚠️ 测速超时：{stream_url[:50]}{'...' if len(stream_url) > 50 else ''}")
    except requests.exceptions.ConnectionError:
        print(f"⚠️ 测速失败：{stream_url[:50]}{'...' if len(stream_url) > 50 else ''}（网络不可达）")
    except Exception as e:
        print(f"⚠️ 测速错误：{stream_url[:50]}{'...' if len(stream_url) > 50 else ''}（{str(e)[:30]}）")
    return None


def batch_test_latency(stream_df: pd.DataFrame, max_workers: int, timeout: int) -> pd.DataFrame:
    """批量测试直播源延迟，返回按延迟排序的有效源DataFrame"""
    if stream_df.empty:
        print("无直播源可测试延迟")
        return pd.DataFrame(columns=["program_name", "stream_url", "latency_ms"])

    total_stream = len(stream_df)
    valid_results = []
    print(f"⚡ 批量测速 | 总流数：{total_stream} | 并发数：{max_workers} | 超时：{timeout}秒")
    print("-" * 100)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_tasks = {
            executor.submit(test_stream_latency, row["stream_url"], timeout): (row["program_name"], row["stream_url"])
            for _, row in stream_df.iterrows()
        }

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

    # 转换为DataFrame并按延迟升序排序
    latency_df = pd.DataFrame(valid_results)
    if not latency_df.empty:
        latency_df = latency_df.sort_values("latency_ms").reset_index(drop=True)

    # 输出测速统计
    print("-" * 100)
    print(f"🏁 测速完成 | 有效流：{len(latency_df)} 个 | 无效流：{total_stream - len(latency_df)} 个")
    if len(latency_df) > 0:
        avg_latency = int(latency_df["latency_ms"].mean())
        print(f"📊 延迟统计 | 最快：{latency_df['latency_ms'].min()}ms | 最慢：{latency_df['latency_ms'].max()}ms | 平均：{avg_latency}ms")
    return latency_df


def organize_streams(content: str, categories: list[dict], all_channels: list) -> list[dict]:
    """按分类模板整理直播源，返回分类后的结构（含前N个低延迟源）"""
    print("\n🔧 开始整理直播源（4个步骤）")
    print("-" * 70)

    # 步骤1：自动识别格式并解析源数据
    if content.startswith("#EXTM3U") or "#EXTINF" in content[:100]:
        print("🔧 步骤1/4：识别为M3U格式，开始解析...")
        parsed_streams = parse_m3u(content)
    else:
        print("🔧 步骤1/4：识别为TXT格式，开始解析...")
        parsed_streams = parse_txt(content)

    stream_df = pd.DataFrame(parsed_streams)
    if stream_df.empty:
        print("整理失败：解析后无有效直播流")
        return []

    # 步骤2：按模板过滤频道（只保留模板中存在的频道）
    print(f"\n🔧 步骤2/4：按模板过滤频道...")
    if "program_name" not in stream_df.columns:
        print("整理失败：解析结果中无program_name字段")
        return []
    stream_df["program_clean"] = stream_df["program_name"].apply(clean_text)
    template_clean = [clean_text(ch) for ch in all_channels]

    # 模糊匹配（兼容频道名细微差异，如"CCTV1"和"CCTV-1"）
    def fuzzy_match(clean_name):
        return any(tpl in clean_name or clean_name in tpl for tpl in template_clean)

    filtered_df = stream_df[stream_df["program_clean"].apply(fuzzy_match)].copy()
    filtered_df = filtered_df.drop_duplicates(subset=["program_name", "stream_url"]).reset_index(drop=True)

    if filtered_df.empty:
        print("整理失败：无匹配模板的频道")
        return []
    print(f"  结果 | 原始：{len(stream_df)} 个 | 匹配：{len(filtered_df)} 个 | 过滤：{len(stream_df)-len(filtered_df)} 个")

    # 步骤3：批量测速
    print(f"\n🔧 步骤3/4：批量测速...")
    valid_df = batch_test_latency(filtered_df[["program_name", "stream_url"]], MAX_SPEED_TEST_WORKERS, SPEED_TEST_TIMEOUT)
    if valid_df.empty:
        print("整理失败：所有源测速失败")
        return []

    # 步骤4：按分类整理（优化排序、接口限制）
    print(f"\n🔧 步骤4/4：按分类整理...")
    organized_data = []
    for cat in categories:
        cat_name = cat["category"]
        cat_ch_clean = [clean_text(ch) for ch in cat["channels"]]
        cat_df = valid_df[valid_df["program_name"].apply(clean_text).isin(cat_ch_clean)].copy()
        if cat_df.empty:
            print(f"分类「{cat_name}」：无有效源，跳过")
            continue

        # 按模板顺序排序（优先模板顺序，再按延迟）
        ch_order = {clean_text(ch): idx for idx, ch in enumerate(cat["channels"])}
        cat_df["program_clean"] = cat_df["program_name"].apply(clean_text)
        cat_df["order"] = cat_df["program_clean"].map(ch_order).fillna(999)
        cat_df_sorted = cat_df.sort_values(["order", "latency_ms"]).reset_index(drop=True)

        # 限制单频道接口数
        def limit_interfaces(group):
            limited = group.head(MAX_INTERFACES_PER_CHANNEL)
            return pd.Series({
                "stream_urls": limited["stream_url"].tolist(),
                "interface_count": len(limited)
            })
        cat_grouped = cat_df_sorted.groupby("program_name").apply(limit_interfaces).reset_index()
        cat_grouped = cat_grouped[cat_grouped["interface_count"] > 0].reset_index(drop=True)

        # 整理分类结果
        cat_result = []
        for _, row in cat_grouped.iterrows():
            cat_result.append({
                "program_name": row["program_name"],
                "interface_count": row["interface_count"],
                "stream_urls": row["stream_urls"]
            })

        organized_data.append({"category": cat_name, "channels": cat_result})

    if not organized_data:
        print("整理失败：无有效分类结果")
        return []

    # 输出整理统计
    total_cats = len(organized_data)
    total_chs = sum(len(cat["channels"]) for cat in organized_data)
    total_ifs = sum(ch["interface_count"] for cat in organized_data for ch in cat["channels"])
    print(f"\n✅ 整理完成 | 分类：{total_cats} 个 | 频道：{total_chs} 个 | 接口：{total_ifs} 个")
    return organized_data


def save_organized_results(organized_data: list[dict]) -> None:
    """保存整理结果为TXT和M3U文件（优化文件结构和信息完整性）"""
    if not organized_data:
        print("无有效数据可保存")
        return

    # 基础统计信息
    total_cats = len(organized_data)
    total_chs = sum(len(cat["channels"]) for cat in organized_data)
    total_ifs = sum(ch["interface_count"] for cat in organized_data for ch in cat["channels"])
    timestamp = time.strftime("%Y%m%d_%H%M%S", time.localtime())
    basic_info = [
        f"# IPTV直播源（按分类整理）",
        f"# 生成时间：{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}",
        f"# 总分类数：{total_cats} | 总频道数：{total_chs} | 总接口数：{total_ifs}",
        f"# 单频道最大接口数：{MAX_INTERFACES_PER_CHANNEL}",
        f"# 测速超时时间：{SPEED_TEST_TIMEOUT}秒"
    ]

    # 1. 保存TXT文件（区分IPv4/IPv6）
    txt_filename = f"{OUTPUT_FILE_PREFIX}_TXT_{timestamp}_限{MAX_INTERFACES_PER_CHANNEL}接口.txt"
    try:
        with open(txt_filename, 'w', encoding='utf-8') as f:
            f.write("\n".join(basic_info) + "\n\n")
            for cat in organized_data:
                cat_ifs_total = sum(ch["interface_count"] for ch in cat["channels"])
                f.write(f"{CATEGORY_MARKER} {cat['category']}\n")
                f.write(f"# 分类频道数：{len(cat['channels'])} | 分类接口数：{cat_ifs_total}\n\n")
                for ch in cat["channels"]:
                    f.write(f"# {ch['program_name']}（{ch['interface_count']}个接口）\n")
                    # 区分IPv4/IPv6
                    ipv4_urls = [url for url in ch['stream_urls'] if IPV4_PATTERN.match(url)]
                    ipv6_urls = [url for url in ch['stream_urls'] if IPV6_PATTERN.match(url)]
                    other_urls = [url for url in ch['stream_urls'] if not (IPV4_PATTERN.match(url) or IPV6_PATTERN.match(url))]
                    # 写入各类型接口
                    if ipv4_urls:
                        f.write("# --- IPv4 接口 ---\n")
                        f.write("\n".join([f"{ch['program_name']},{url}" for url in ipv4_urls]) + "\n\n")
                    if ipv6_urls:
                        f.write("# --- IPv6 接口 ---\n")
                        f.write("\n".join([f"{ch['program_name']},{url}" for url in ipv6_urls]) + "\n\n")
                    if other_urls:
                        f.write("# --- 其他 接口 ---\n")
                        f.write("\n".join([f"{ch['program_name']},{url}" for url in other_urls]) + "\n\n")
        print(f"\n📄 TXT文件保存成功 | 路径：{os.path.abspath(txt_filename)}")
    except Exception as e:
        print(f"❌ TXT文件保存失败：{str(e)}")

    # 2. 保存M3U文件（优化播放器兼容性）
    m3u_filename = f"{OUTPUT_FILE_PREFIX}_M3U_{timestamp}_限{MAX_INTERFACES_PER_CHANNEL}接口.m3u"
    try:
        with open(m3u_filename, 'w', encoding='utf-8') as f:
            f.write("#EXTM3U\n")
            f.write("\n".join(basic_info[1:]) + "\n\n")  # 去掉首行（M3U标准）
            for cat in organized_data:
                f.write(f"# {CATEGORY_MARKER} {cat['category']}\n")
                for ch in cat["channels"]:
                    ch_remark = f"频道：{ch['program_name']} | 接口数：{ch['interface_count']}"
                    f.write(f"# {ch_remark}\n")
                    for idx, url in enumerate(ch['stream_urls'], 1):
                        # 增加tvg-id和tvg-logo占位（提升播放器显示效果）
                        f.write(f'#EXTINF:-1 tvg-id="{ch['program_name']}" tvg-name="{ch['program_name']}" tvg-logo="" group-title="{cat['category']}",{ch['program_name']}_{idx}\n')
                        f.write(f"{url}\n")
                f.write("\n")
        print(f"📺 M3U文件保存成功 | 路径：{os.path.abspath(m3u_filename)}")
    except Exception as e:
        print(f"❌ M3U文件保存失败：{str(e)}")


if __name__ == "__main__":
    print_separator("IPTV直播源分类整理工具（最终修复版）")
    
    try:
        # 步骤1：读取分类模板
        print("\n【步骤1：读取分类模板】")
        categories, all_channels = read_category_template(CATEGORY_TEMPLATE_PATH)
        if not categories or not all_channels:
            raise Exception("模板读取失败")

        # 步骤2：批量抓取直播源
        print("\n【步骤2：批量抓取直播源】")
        raw_content = batch_fetch_sources(SOURCE_URLS)
        if not raw_content.strip():
            raise Exception("未抓取到任何直播源内容")

        # 步骤3：按分类整理直播源
        print("\n【步骤3：按分类整理直播源】")
        organized_data = organize_streams(raw_content, categories, all_channels)
        if not organized_data:
            raise Exception("直播源整理失败")

        # 步骤4：保存结果文件
        print("\n【步骤4：保存结果文件】")
        save_organized_results(organized_data)

        print_separator("流程完成")
        print("🎉 所有操作执行完成！结果文件已保存至当前目录")
    except Exception as e:
        print_separator("流程终止")
        print(f"❌ 流程终止：{str(e)}")
        exit(1)
