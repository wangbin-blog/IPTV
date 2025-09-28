import requests
import pandas as pd
import re
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# 配置区：可根据需求修改
URLS = [
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
DEMO_PATH = "demo.txt"  # 带分类的模板文件路径
MAX_INTERFACES_PER_CHANNEL = 8  # 单频道最大接口数
SPEED_TEST_TIMEOUT = 10  # 测速超时（秒）
MAX_WORKERS = 15  # 测速并发数
FILENAME_PREFIX = "iptv"  # 输出文件前缀
CATEGORY_MARKER = "##"  # 分类标记（以##开头的行为分类名）

# 正则表达式
IPV4_PATTERN = re.compile(r'^http://(\d{1,3}\.){3}\d{1,3}')
IPV6_PATTERN = re.compile(r'^http://\[([a-fA-F0-9:]+)\]')
URL_PATTERN = re.compile(r'^https?://')
SPACE_PATTERN = re.compile(r'\s+')


def read_demo_with_categories(demo_path: str) -> tuple[list[dict], list[str]] | tuple[None, None]:
    """读取带分类的demo模板，返回分类结构和纯频道列表"""
    if not os.path.exists(demo_path):
        print(f"❌ 错误：模板文件 '{demo_path}' 不存在！")
        return None, None
    
    categories = []  # 分类结构：[{"category": "分类名", "channels": ["频道1", "频道2"]}]
    current_category = None
    demo_channels = []  # 纯频道列表（去重，用于过滤）
    
    with open(demo_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            # 跳过空行和普通注释（#开头，非分类标记）
            if not line:
                continue
            if line.startswith("#") and not line.startswith(CATEGORY_MARKER):
                continue
            
            # 识别分类行（##开头）
            if line.startswith(CATEGORY_MARKER):
                current_category = SPACE_PATTERN.sub("", line.lstrip(CATEGORY_MARKER).strip())
                if current_category:
                    categories.append({"category": current_category, "channels": []})
                continue
            
            # 处理频道行（属于当前分类）
            if current_category is None:
                print(f"⚠️ 第{line_num}行：频道未指定分类，默认归为「未分类」")
                if not any(c["category"] == "未分类" for c in categories):
                    categories.append({"category": "未分类", "channels": []})
                current_category = "未分类"
            
            # 提取频道名（兼容“频道名,URL”格式）
            channel = SPACE_PATTERN.sub("", line.split(",")[0].strip())
            if channel and channel not in demo_channels:
                demo_channels.append(channel)
                # 将频道加入当前分类
                for cat in categories:
                    if cat["category"] == current_category:
                        cat["channels"].append(channel)
                        break
    
    # 验证分类结构
    if not categories:
        print("⚠️ 警告：模板文件无有效分类和频道")
        return None, None
    total_channels = sum(len(c["channels"]) for c in categories)
    print(f"📺 从模板读取分类：{len(categories)} 个分类，共 {total_channels} 个有效频道")
    
    # 打印分类详情
    for i, cat in enumerate(categories, 1):
        print(f"  {i}. {cat['category']}：{len(cat['channels'])} 个频道")
    
    return categories, demo_channels


def fetch_streams_from_url(url: str) -> str | None:
    print(f"\n🔍 正在爬取源：{url}")
    try:
        response = requests.get(
            url,
            timeout=10,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        )
        response.encoding = response.apparent_encoding
        if response.status_code == 200:
            print(f"✅ 爬取成功，内容长度：{len(response.text)} 字符")
            return response.text
        print(f"❌ 爬取失败，状态码：{response.status_code}")
    except requests.exceptions.Timeout:
        print(f"❌ 请求超时（超过10秒）")
    except requests.exceptions.ConnectionError:
        print(f"❌ 连接错误")
    except Exception as e:
        print(f"❌ 未知错误：{str(e)[:50]}")
    return None


def fetch_all_streams(urls: list) -> str:
    all_content = []
    for url in urls:
        if content := fetch_streams_from_url(url):
            all_content.append(content)
        else:
            print(f"⏭️  跳过无效源：{url}")
    return "\n".join(all_content)


def parse_m3u(content: str) -> list[dict]:
    streams = []
    current_program = None
    for line in content.splitlines():
        line = line.strip()
        if line.startswith("#EXTINF"):
            if match := re.search(r'tvg-name=(["\']?)([^"\']+)\1', line):
                current_program = SPACE_PATTERN.sub("", match.group(2).strip())
        elif URL_PATTERN.match(line) and current_program:
            streams.append({"program_name": current_program, "stream_url": line})
            current_program = None
    print(f"📊 解析M3U格式：提取到 {len(streams)} 个直播源")
    return streams


def parse_txt(content: str) -> list[dict]:
    streams = []
    for line in content.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if match := re.match(r'(.+?)\s*,\s*(https?://.+)$', line):
            program = SPACE_PATTERN.sub("", match.group(1).strip())
            url = match.group(2).strip()
            streams.append({"program_name": program, "stream_url": url})
    print(f"📊 解析TXT格式：提取到 {len(streams)} 个直播源")
    return streams


def test_stream_speed(stream_url: str, timeout: int) -> int | None:
    start_time = time.time()
    try:
        for method in [requests.head, requests.get]:
            try:
                kwargs = {"timeout": timeout, "allow_redirects": True}
                if method == requests.get:
                    kwargs["stream"] = True
                response = method(stream_url, **kwargs)
                if response.status_code in [200, 206]:
                    if method == requests.get:
                        response.iter_content(1).__next__()
                    return int((time.time() - start_time) * 1000)
            except:
                continue
        return None
    except Exception:
        return None


def batch_test_speeds(streams_df: pd.DataFrame, max_workers: int, timeout: int) -> pd.DataFrame:
    total = len(streams_df)
    if total == 0:
        return pd.DataFrame()
    
    print(f"\n⚡ 开始测速（共 {total} 个源，并发数：{max_workers}，超时：{timeout}秒）")
    speed_results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(test_stream_speed, row["stream_url"], timeout):
            (row["program_name"], row["stream_url"])
            for _, row in streams_df.iterrows()
        }
        
        for idx, future in enumerate(as_completed(futures), 1):
            program, url = futures[future]
            speed = future.result()
            url_short = url[:50] + "..." if len(url) > 50 else url
            
            if speed is not None:
                speed_results.append({"program_name": program, "stream_url": url, "speed_ms": speed})
                print(f"✅ [{idx}/{total}] {program:<15} {url_short:<55} 耗时：{speed}ms")
            else:
                print(f"❌ [{idx}/{total}] {program:<15} {url_short:<55} 超时/不可用")
    
    speed_df = pd.DataFrame(speed_results)
    if not speed_df.empty:
        speed_df = speed_df.sort_values("speed_ms").reset_index(drop=True)
    
    print(f"\n🏁 测速完成：有效源 {len(speed_df)} 个，无效源 {total - len(speed_df)} 个")
    return speed_df


def organize_streams(
    content: str,
    categories: list[dict],
    demo_channels: list,
    max_interfaces: int,
    max_workers: int,
    speed_timeout: int
) -> list[dict]:
    """按分类整理数据，返回带分类的结构化数据"""
    # 1. 解析原始数据
    if content.startswith("#EXTM3U"):
        streams = parse_m3u(content)
    else:
        streams = parse_txt(content)
    df = pd.DataFrame(streams)
    if df.empty:
        print("⚠️ 未解析到任何直播源")
        return []
    
    # 2. 过滤+去重
    df["program_clean"] = df["program_name"].apply(lambda x: SPACE_PATTERN.sub("", x))
    demo_clean = demo_channels
    df_filtered = df[df["program_clean"].isin(demo_clean)].drop("program_clean", axis=1)
    df_filtered = df_filtered.drop_duplicates(subset=["program_name", "stream_url"])
    
    if df_filtered.empty:
        print("⚠️ 无匹配demo模板的直播源")
        return []
    print(f"\n🔍 过滤后剩余 {len(df_filtered)} 个匹配模板的直播源")
    
    # 3. 批量测速
    df_with_speed = batch_test_speeds(df_filtered, max_workers, speed_timeout)
    if df_with_speed.empty:
        print("⚠️ 所有匹配源均测速失败")
        return []
    
    # 4. 按分类+频道排序，限制接口数
    organized_categories = []
    for cat in categories:
        # 筛选当前分类的频道
        cat_channels = cat["channels"]
        df_cat = df_with_speed[df_with_speed["program_name"].isin(cat_channels)]
        
        if df_cat.empty:
            continue  # 跳过无有效源的分类
        
        # 按模板中频道顺序排序
        df_cat["program_name"] = pd.Categorical(
            df_cat["program_name"], categories=cat_channels, ordered=True
        )
        df_cat_sorted = df_cat.sort_values(["program_name", "speed_ms"]).reset_index(drop=True)
        
        # 按频道分组，限制接口数
        def limit_interfaces(group):
            limited = group.head(max_interfaces)
            return pd.Series({
                "stream_url": limited["stream_url"].tolist(),
                "interface_count": len(limited)
            })
        
        df_cat_grouped = df_cat_sorted.groupby("program_name").apply(limit_interfaces).reset_index()
        df_cat_grouped = df_cat_grouped[df_cat_grouped["interface_count"] > 0]
        
        if not df_cat_grouped.empty:
            organized_categories.append({
                "category": cat["category"],
                "channels": df_cat_grouped.to_dict("records")  # 每个频道的URL和接口数
            })
    
    return organized_categories


def save_to_txt(organized_categories: list[dict], prefix: str, max_interfaces: int) -> None:
    if not organized_categories:
        return
    
    # 计算总接口数
    total_interfaces = 0
    for cat in organized_categories:
        total_interfaces += sum(ch["interface_count"] for ch in cat["channels"])
    
    filename = f"{prefix}_分类_单频道限{max_interfaces}_总接口{total_interfaces}.txt"
    content_lines = [f"# IPTV直播源（按分类整理）", f"# 总分类数：{len(organized_categories)}，总接口数：{total_interfaces}", ""]
    
    for cat in organized_categories:
        # 分类标题
        content_lines.append(f"\n{CATEGORY_MARKER} {cat['category']}")
        content_lines.append(f"# 分类下有效频道数：{len(cat['channels'])}")
        
        # 按IPv4/IPv6分组
        cat_ipv4 = []
        cat_ipv6 = []
        for ch in cat["channels"]:
            program = ch["program_name"]
            count = ch["interface_count"]
            urls = ch["stream_url"]
            note = f"# {program}（保留：{count}/{max_interfaces}个）"
            
            for url in urls:
                line = f"{program},{url}"
                if IPV4_PATTERN.match(url):
                    cat_ipv4.append((note, line))
                    note = ""  # 只添加一次频道注释
                elif IPV6_PATTERN.match(url):
                    cat_ipv6.append((note, line))
                    note = ""
        
        # 添加IPv4内容
        if cat_ipv4:
            content_lines.append("\n# --- IPv4 源 ---")
            for note, line in cat_ipv4:
                if note:
                    content_lines.append(note)
                content_lines.append(line)
        
        # 添加IPv6内容
        if cat_ipv6:
            content_lines.append("\n# --- IPv6 源 ---")
            for note, line in cat_ipv6:
                if note:
                    content_lines.append(note)
                content_lines.append(line)
    
    # 写入文件
    with open(filename, 'w', encoding='utf-8') as f:
        f.write("\n".join([line for line in content_lines if line]))
    
    print(f"\n📄 TXT文件已保存：{os.path.abspath(filename)}")


def save_to_m3u(organized_categories: list[dict], prefix: str, max_interfaces: int) -> None:
    if not organized_categories:
        return
    
    total_interfaces = 0
    for cat in organized_categories:
        total_interfaces += sum(ch["interface_count"] for ch in cat["channels"])
    
    filename = f"{prefix}_分类_单频道限{max_interfaces}_总接口{total_interfaces}.m3u"
    
    with open(filename, 'w', encoding='utf-8') as f:
        f.write("#EXTM3U\n")
        f.write(f"# IPTV直播源（按分类整理）\n")
        f.write(f"# 总分类数：{len(organized_categories)}，总接口数：{total_interfaces}\n")
        f.write(f"# 单频道最多保留{max_interfaces}个接口，同频道按速度排序\n\n")
        
        for cat in organized_categories:
            # 分类注释（用##标记，播放器忽略）
            f.write(f"# {CATEGORY_MARKER} {cat['category']}\n")
            f.write(f"# 分类下有效频道数：{len(cat['channels'])}\n\n")
            
            for ch in cat["channels"]:
                program = ch["program_name"]
                count = ch["interface_count"]
                urls = ch["stream_url"]
                f.write(f"# {program}（保留：{count}/{max_interfaces}个）\n")
                
                for url in urls:
                    f.write(f'#EXTINF:-1 tvg-name="{program}",{program}\n')
                    f.write(f"{url}\n\n")
    
    print(f"📺 M3U文件已保存：{os.path.abspath(filename)}")


def main():
    print("=" * 60)
    print("📡 IPTV直播源抓取整理工具（分类版）")
    print("=" * 60)
    
    # 1. 读取带分类的模板
    categories, demo_channels = read_demo_with_categories(DEMO_PATH)
    if not categories or not demo_channels:
        print("\n❌ 程序终止：缺少有效分类/频道")
        return
    
    # 2. 抓取直播源
    print("\n" + "-" * 60)
    all_content = fetch_all_streams(URLS)
    if not all_content.strip():
        print("\n❌ 程序终止：未抓取到有效内容")
        return
    
    # 3. 按分类整理数据
    print("\n" + "-" * 60)
    organized = organize_streams(
        content=all_content,
        categories=categories,
        demo_channels=demo_channels,
        max_interfaces=MAX_INTERFACES_PER_CHANNEL,
        max_workers=MAX_WORKERS,
        speed_timeout
