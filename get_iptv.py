import requests
import pandas as pd
import re
import os

# 配置参数
ONLINE_URLS = [
    "https://raw.githubusercontent.com/zwc456baby/iptv_alive/master/live.txt",
    "https://live.zbds.top/tv/iptv6.txt",
    "https://live.zbds.top/tv/iptv4.txt",
]
# 模板文件（用于过滤和排序）
TEMPLATE_FILE = "demo.txt"
# 输出文件
OUTPUT_TXT = "live.txt"
OUTPUT_M3U = "live.m3u"
# 正则匹配规则（扩展支持http/https/rtsp）
IPV4_PATTERN = re.compile(r'^(http|https|rtsp)://(\d{1,3}\.){3}\d{1,3}')
IPV6_PATTERN = re.compile(r'^(http|https|rtsp)://\[([a-fA-F0-9:]+)\]')

def fetch_streams_from_url(url):
    """抓取单个在线源"""
    print(f"正在爬取在线源: {url}")
    try:
        response = requests.get(url, timeout=10)
        response.encoding = 'utf-8'
        if response.status_code == 200:
            return response.text
        print(f"在线源 {url} 失败，状态码: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"在线源 {url} 错误: {e}")
    return None

def fetch_all_online_streams():
    """抓取所有在线源并合并"""
    all_content = []
    for url in ONLINE_URLS:
        if content := fetch_streams_from_url(url):
            all_content.append(content)
        else:
            print(f"跳过无效在线源: {url}")
    return "\n".join(all_content)

def read_template_file(filename=TEMPLATE_FILE):
    """读取demo.txt模板，返回{频道名: 排序索引}字典和频道列表"""
    print(f"正在读取模板文件: {filename}")
    template_channels = []
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            for line in f.read().splitlines():
                line = line.strip()
                if not line or line.startswith("#"):  # 跳过注释和空行
                    continue
                # 提取模板中的频道名（忽略链接，只取逗号前内容）
                if "," in line:
                    channel = line.split(",")[0].strip()
                    if channel:
                        template_channels.append(channel)
        if not template_channels:
            print(f"警告：模板文件 {filename} 中未找到有效频道")
            return None, None
        # 创建频道名到排序索引的映射
        channel_order = {chan: idx for idx, chan in enumerate(template_channels)}
        print(f"模板解析完成，共 {len(template_channels)} 个目标频道")
        return channel_order, template_channels
    except FileNotFoundError:
        print(f"错误：未找到模板文件 {filename}，程序终止")
        return None, None
    except Exception as e:
        print(f"读取模板文件错误: {e}，程序终止")
        return None, None

def parse_content(content):
    """解析在线源内容（支持M3U/TXT）"""
    streams = []
    if content.startswith("#EXTM3U"):
        # 解析M3U格式
        current_program = None
        for line in content.splitlines():
            line = line.strip()
            if line.startswith("#EXTINF"):
                if match := re.search(r'tvg-name="([^"]+)"', line):
                    current_program = match.group(1).strip()
            elif line.startswith(("http", "rtsp")) and current_program:
                streams.append({"program_name": current_program, "stream_url": line})
                current_program = None
    else:
        # 解析TXT格式
        for line in content.splitlines():
            line = line.strip()
            if match := re.match(r"(.+?),\s*(http|https|rtsp.+)", line):
                streams.append({
                    "program_name": match.group(1).strip(),
                    "stream_url": match.group(2).strip()
                })
    return streams

def filter_and_sort_streams(streams, channel_order):
    """按模板过滤频道，并按模板顺序排序"""
    # 转为DataFrame去重
    df = pd.DataFrame(streams).drop_duplicates(subset=['program_name', 'stream_url'])
    # 过滤：只保留模板中存在的频道
    df = df[df['program_name'].isin(channel_order.keys())]
    if df.empty:
        print("警告：未找到与模板匹配的频道")
        return None
    # 按模板顺序排序（添加排序索引列，排序后删除）
    df['sort_idx'] = df['program_name'].map(channel_order)
    df_sorted = df.sort_values('sort_idx').drop('sort_idx', axis=1)
    # 按频道名分组，聚合链接
    grouped = df_sorted.groupby('program_name')['stream_url'].apply(list).reset_index()
    print(f"过滤排序完成，共匹配到 {len(grouped)} 个模板频道")
    return grouped

def save_to_live_txt(grouped_streams):
    """保存为live.txt（按IPv4/IPv6分类）"""
    ipv4 = []
    ipv6 = []
    other = []
    for _, row in grouped_streams.iterrows():
        program = row['program_name']
        for url in row['stream_url']:
            if IPV4_PATTERN.match(url):
                ipv4.append(f"{program},{url}")
            elif IPV6_PATTERN.match(url):
                ipv6.append(f"{program},{url}")
            else:
                other.append(f"{program},{url}")
    with open(OUTPUT_TXT, 'w', encoding='utf-8') as f:
        if ipv4:
            f.write("# IPv4 Streams\n" + "\n".join(ipv4) + "\n\n")
        if ipv6:
            f.write("# IPv6 Streams\n" + "\n".join(ipv6) + "\n\n")
        if other:
            f.write("# Other Streams\n" + "\n".join(other))
    print(f"live.txt 已保存: {os.path.abspath(OUTPUT_TXT)}")

def save_to_live_m3u(grouped_streams):
    """保存为live.m3u（按模板顺序）"""
    with open(OUTPUT_M3U, 'w', encoding='utf-8') as f:
        f.write("#EXTM3U\n")
        for _, row in grouped_streams.iterrows():
            program = row['program_name']
            for url in row['stream_url']:
                f.write(f'#EXTINF:-1 tvg-name="{program}",{program}\n{url}\n')
    print(f"live.m3u 已保存: {os.path.abspath(OUTPUT_M3U)}")

if __name__ == "__main__":
    print("=== 启动IPTV模板过滤排序工具 ===")
    # 1. 读取模板文件（获取过滤规则和排序顺序）
    channel_order, template_channels = read_template_file()
    if not channel_order:
        exit()
    # 2. 抓取在线源
    online_content = fetch_all_online_streams()
    if not online_content:
        print("错误：未获取到任何在线源数据")
        exit()
    # 3. 解析在线源
    parsed_streams = parse_content(online_content)
    if not parsed_streams:
        print("错误：未解析到有效直播源")
        exit()
    # 4. 按模板过滤并排序
    sorted_streams = filter_and_sort_streams(parsed_streams, channel_order)
    if not sorted_streams:
        exit()
    # 5. 保存输出文件
    save_to_live_txt(sorted_streams)
    save_to_live_m3u(sorted_streams)
    print("=== 所有操作完成 ===")