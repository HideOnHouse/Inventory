import os
import datetime
import multiprocessing as mp


from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

SUBS = {
    2294: ['히어로', '팔라딘', '다크나이트', '소울마스터', '아란', '데몬슬레이어', '미하일', '카이저', '데몬어벤져', '제로', '블래스터', '아델'],
    2295: ['아크(불독)', '아크(썬콜)', '비숍', '플레임위자드', '에반', '배틀메이지', '루미너스', '키네시스', '일리움', '라라'],
    2296: ['보우마스터', '신궁', '윈드브레이커', '와일드헌터', '메르세데스', '패스파인더', '카인'],
    2297: ['나이트로드', '섀도어', '나이트워커', '듀얼블래이드', '괴도팬텀', '카데나', '호영'],
    2298: ['메카닉', '바이퍼', '캡틴', '스트라이커', '캐논슈터', '엔젤릭버스터', '제논', '은월', '아크']
}

"""
https://www.inven.co.kr/board/maple/2294
https://www.inven.co.kr/board/maple/2294?category={category}

"""

def crawl_job(job, sub, num_page, save_path):
    # load selenium
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    now = datetime.datetime.now()

    table = [[], [], [], [], []]
    for page in range(1, 501):
        break_flag = False
        driver.get(f"https://www.inven.co.kr/board/maple/{job}?category={sub}&p={page}")
        for idx, elem in enumerate(['subject-link', 'layerNickName', 'date', 'view', 'reco']):
            src = driver.find_elements(by=By.CLASS_NAME, value=elem)
            if len(src) == 0:
                break_flag = True
                break
            src = [i.text for i in src]
            if elem == 'date':
                src = [f"{now.month:0>2}-{now.day:0>2}" if ':' in i else i for i in src]
            table[idx].extend(src)
        if break_flag:
            break
    with open(f"./{save_path}/{now.year}{now.month}{now.day}_{sub}.tsv", 'w', encoding='utf-8') as f:
        f.write("title\tuser\ttime\tview\trecommendation\n")
        for row in zip(*table):
            f.write("\t".join(row) + '\n')

def main():
    # configure save directory
    now = datetime.datetime.now()
    save_path = f"./{now.year:0>4}{now.month:0>2}{now.day:0>2}"
    while os.path.exists(save_path):
        new_path = save_path.split(" ")
        if len(new_path) == 1:
            prefix = 1
        else:
            prefix = int(new_path[1][1]) + 1
        save_path = new_path[0] + f" ({prefix})"
    os.makedirs(save_path, exist_ok=False)

    # prepare args
    args = []
    for job in [2294, 2295, 2296, 2297, 2298]:
        for sub in SUBS[job]:
            args.append([job, sub, 500, save_path])
    with mp.Pool(processes=4) as pool:
        res = pool.starmap_async(crawl_job, args)
        res.wait()
    
if __name__ == '__main__':
    main()
