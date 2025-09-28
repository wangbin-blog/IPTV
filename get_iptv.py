import requests
import pandas as pd
import re
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

urls = [
    "https://raw.githubusercontent.com/zwc456baby/iptv_alive/master/live.txt",
    "http://47.120.41.246:8899/zb.txt",
    "https://iptv.mydiver.eu.org/get.php?username=tg_442l98bq&password=4u6yo6fx7a4q&type=m3u_plus",
    "https://ghfast.top/raw.githubusercontent.com/Supprise0901/TVBox_live/main/live.txt",
    "https://gh-proxy.com/https://raw.githubusercontent.com/wwb521/live/main/tv.m3u",
    "https://gh-proxy.com/https://raw.githubusercontent.com/zeee-u/lzh06/main/fl.m3u",
    "https://raw.githubusercontent.com/Guovin/iptv-database/master/result.txt",  
    "https://live.zbds.top/tv/iptv4.txt",
]

ipv4_pattern = re.compile(r'^http://(\d{1,3}\.){3}\d{1,3}')
ipv6_pattern = re.compile(r'^http://\[([a-fA-F0-9:]+)\]')
# 增加对https和其他端口的兼容
url_pattern = re.compile(r'^https?://')

def read_demo_channels(demo_path="demo.txt"):
    if not os.path.exists(demo_path):
        print(f"错误：模板文件 {demo_path} 不存在！")
        return None
    demo_channels = []
    with open(demo_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            channel = line.split(",")[0].strip()
            if channel not in demo_channels:
                demo_channels.append(channel)
    print(f"从模板文件读取到 {len(demo_channels)} 个有效频道")
    return demo_channels

def fetch_streams_from_url(url):
    print(f"正在爬取网站源: {url}")
    try:
        response = requests.get(url, timeout=10)
        response.encoding = 'utf-8'
        if response.status_code == 200:
            return response.text
        print(f"从 {url} 获取数据失败，状态码: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"请求 {url} 时发生错误: {e}")
    return None

def fetch_all_streams():
    all_streams = []
    for url in urls:
        if content := fetch_streams_from_url(url):
            all_streams.append(content)
        else:
            print(f"跳过来源: {url}")
    return "\n".join(all_streams)

def parse_m3u(content):
    streams = []
    current_program = None
    for line in content.splitlines():
        if line.startswith("#EXTINF"):
            if match := re.search(r'tvg-name="([^"]+)"', line):
                current_program = match.group(1).strip()
        elif url_pattern.match(line):
            if current_program:
                streams.append({"program_name": current_program, "stream_url": line.strip()})
                current_program = None
    return streams

def parse_txt(content):
    streams = []
    for line in content.splitlines():
        if match := re.match(r"(.+?),\s*(https?://.+)", line):
            streams.append({
                "program_name": match.group(1).strip(),
                "stream_url": match.group(2).strip()
            })
    return streams

def test_stream_speed(stream_url, timeout=5):
    """测试单个直播源的响应时间（毫秒），超时/失败返回None"""
    start_time = time.time()
    try:
        # 优先用HEAD请求（轻量），若不支持则用GET请求并只获取1字节
        response = requests.head(stream_url, timeout=timeout, allow_redirects=True)
        if response.status_code not in [200, 206]:
            # HEAD失败时尝试GET片段
            response = requests.get(stream_url, timeout=timeout, allow_redirects=True, stream=True)
            response.iter_content(1).__next__()  # 只读取1字节
        elapsed = int((time.time() - start_time) * 1000)
        return elapsed
    except (requests.exceptions.RequestException, StopIteration):
        return None

def batch_test_speeds(streams_df, max_workers=10):
    """批量测试直播源速度，返回带速度的DataFrame"""
    print(f"\n开始测速（共 {len(streams_df)} 个直播源，并发数：{max_workers}）...")
    # 为每个源创建任务
    speed_results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有任务并记录future与对应的行数据
        futures = {
            executor.submit(test_stream_speed, row["stream_url"]): (row["program_name"], row["stream_url"])
            for _, row in streams_df.iterrows()
        }
        # 实时获取结果
        for future in as_completed(futures):
            program, url = futures[future]
            speed = future.result()
            if speed is not None:
                speed_results.append({"program_name": program, "stream_url": url, "speed_ms": speed})
                print(f"✅ {program} - {url[:50]:<50} 响应时间：{speed}ms")
            else:
                print(f"❌ {program} - {url[:50]:<50} 超时/不可用")
    # 转换为DataFrame并按速度排序（升序，快的在前）
    speed_df = pd.DataFrame(speed_results)
    if not speed_df.empty:
        speed_df = speed_df.sort_values("speed_ms").reset_index(drop=True)
    print(f"\n测速完成，有效直播源：{len(speed_df)} 个（过滤掉 {len(streams_df)-len(speed_df)} 个不可用源）")
    return speed_df

def organize_streams(content, demo_channels):
    # 解析原始数据
    parser = parse_m3u if content.startswith("#EXTM3U") else parse_txt
    df = pd.DataFrame(parser(content))
    if df.empty:
        return pd.DataFrame(columns=["program_name", "stream_url"])
    
    # 1. 过滤：只保留demo中的频道
    df = df[df["program_name"].isin(demo_channels)]
    # 2. 去重
    df = df.drop_duplicates(subset=['program_name', 'stream_url'])
    if df.empty:
        return df
    
    # 3. 批量测速
    df_with_speed = batch_test_speeds(df)
    if df_with_speed.empty:
        return df_with_speed
    
    # 4. 按demo顺序排序（先按频道顺序，再按速度排序）
    df_with_speed["program_name"] = pd.Categorical(
        df_with_speed["program_name"], categories=demo_channels, ordered=True
    )
    df_with_speed = df_with_speed.sort_values(["program_name", "speed_ms"]).reset_index(drop=True)
    # 5. 按节目名分组，整合URL列表（快的URL在前）
    grouped_df = df_with_speed.groupby('program_name')['stream_url'].apply(list).reset_index()
    return grouped_df

def save_to_txt(grouped_streams, filename="iptv.txt"):
    if grouped_streams.empty:
        print("无符合条件的直播源可保存到TXT")
        return
    ipv4 = []
    ipv6 = []
    for _, row in grouped_streams.iterrows():
        program = row['program_name']
        for url in row['stream_url']:
            if ipv4_pattern.match(url):
                ipv4.append(f"{program},{url}")
            elif ipv6_pattern.match(url):
                ipv6.append(f"{program},{url}")
    with open(filename, 'w', encoding='utf-8') as f:
        f.write("# IPv4 Streams（按速度排序，快的在前）\n" + "\n".join(ipv4))
        f.write("\n\n# IPv6 Streams（按速度排序，快的在前）\n" + "\n".join(ipv6))
    print(f"文本文件已保存: {os.path.abspath(filename)}")

def save_to_m3u(grouped_streams, filename="iptv.m3u"):
    if grouped_streams.empty:
        print("无符合条件的直播源可保存到M3U")
        return
    with open(filename, 'w', encoding='utf-8') as f:
        f.write("#EXTM3U\n")
        for _, row in grouped_streams.iterrows():
            program = row['program_name']
            # 每个频道的URL按速度排序，快的在前
            for url in row['stream_url']:
                f.write(f'#EXTINF:-1 tvg-name="{program}",{program}\n{url}\n')
    print(f"M3U文件已保存: {os.path.abspath(filename)}")

if __name__ == "__main__":
    # 1. 读取demo模板频道
    demo_channels = read_demo_channels()
    if not demo_channels:
        print("无法继续，缺少有效模板频道列表")
        exit(1)
    
    # 2. 抓取并整理直播源（过滤+测速+排序）
    print("\n开始抓取所有源...")
    if content := fetch_all_streams():
        print("整理源数据中（按模板过滤→测速→排序）...")
        organized = organize_streams(content, demo_channels)
        if organized.empty:
            print("整理后无符合条件的直播源")
        else:
            save_to_txt(organized)
            save_to_m3u(organized)
    else:
        print("未能获取有效数据")
