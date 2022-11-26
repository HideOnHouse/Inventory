import os

import pandas as pd


def build_dataframe(base_path: str) -> pd.DataFrame:
    temp = {
        '전사': ['히어로', '팔라딘', '다크나이트', '소울마스터', '아란', '데몬슬레이어', '미하일', '카이저', '데몬어벤져', '제로', '블래스터', '아델'],
        '마법사': ['아크(불독)', '아크(썬콜)', '비숍', '플레임위자드', '에반', '배틀메이지', '루미너스', '키네시스', '일리움', '라라'],
        '궁수': ['보우마스터', '신궁', '윈드브레이커', '와일드헌터', '메르세데스', '패스파인더', '카인'],
        '도적': ['나이트로드', '섀도어', '나이트워커', '듀얼블래이드', '괴도팬텀', '카데나', '호영'],
        '해적': ['메카닉', '바이퍼', '캡틴', '스트라이커', '캐논슈터', '엔젤릭버스터', '제논', '은월', '아크']
    }
    subs = {}
    for k, v in temp.items():
        for i in v:
            subs[i] = k

    ret = pd.DataFrame()
    for file in os.listdir(base_path):
        ret = pd.concat(
            [ret, pd.read_csv(base_path + os.sep + file, sep='\t', thousands=',')])
    # remove invalid data
    ret = ret[ret['time'].apply(lambda x: len(x) == 5)]
    ret = ret.sort_values(by='time')
    ret = ret.reset_index().drop(columns=['index'])
    ret['job'] = ret['title'].apply(lambda x: x[x.index('[')+1:x.index(']')])
    ret['category'] = ret['job'].apply(lambda x: subs[x])
    ret['title'] = ret['title'].apply(lambda x: x.split(']')[1].strip())

    ret = ret.astype({'title': str,
                      'user': str,
                      'time': str,
                      'view': int,
                      'recommendation': int,
                      'job': str,
                      'category': str,
                      })

    return ret


def get_event_date() -> dict:
    ret = {
        '01-18': '모험가 리마스터 T',
        '01-27': '모험가 리마스터',
        '02-24': '칼로스, 에테르넬',
        '03-24': '앰배서더 지수',
        '04-21': '모멘트리',
        '06-11': 'IGNITION 쇼케이스',
        '06-23': 'IGNITION, 시그너스 리마스터 T',
        "06-30": "시그너스 리마스터, 하이퍼 버닝, 봄봄, 이그니션",
        "07-28": "익스트림, 어메이징 이그니션",
        "08-18": "오디움, 대규모 밸패 T",
        "08-25": "오디움, 대규모 밸패",
        "09-29": "택티컬, 헤이스트",
        "10-27": "핑크빈, 헤이스트, 골드리치, 크로니클",
        '11-17': "키워달람 T"
    }
    return ret


def word_counter(x: str):
    ret = dict()
    x = x.split(" ")
    for tok in x:
        if '●' in tok:
            tok = '!시체!'
        if tok not in ret:
            ret[tok] = 0
        ret[tok] += 1
    return ret


def word_cnt_by(df: pd.DataFrame, by: str = None) -> dict:
    if by is None:
        ret = " ".join(df['title'])
        ret = word_counter(ret)
    if by in {'category', 'job'}:
        ret = df.groupby(by)['title'].sum()
        ret = ret.apply(lambda x: word_counter(x)).to_dict()
    elif by in {'time'}:
        ret = df.groupby('time')['title'].sum()
        ret = ret.apply(lambda x: sorted(word_counter(x).items(),
                        key=lambda x: x[1], reverse=True)[0]).reset_index()
        ret['count'] = ret['title'].apply(lambda x: x[1])
        ret['title'] = ret['title'].apply(lambda x: x[0])
        ret = ret.sort_values(by='time', ascending=True)
        ret = ret.to_dict()
    return ret


def pred_sentiment(df: pd.DataFrame, batch_size=512) -> dict:
    from transformers import AutoTokenizer, AutoModelForSequenceClassification
    import torch
    tokenizer = AutoTokenizer.from_pretrained(
        "sangrimlee/bert-base-multilingual-cased-nsmc")
    model = AutoModelForSequenceClassification.from_pretrained(
        "sangrimlee/bert-base-multilingual-cased-nsmc")
    ret = {'pred': [], 'score': []}
    batch_size = 512
    device = 'cuda' if torch.cuda.is_available() else 'cpu'
    with torch.no_grad():
        model.eval()
        model.to(device)
        for batch_id in range(0, len(df), batch_size):
            sentences = df[batch_id:batch_id + batch_size]['title'].tolist()
            tok = tokenizer(sentences, return_tensors='pt',
                            padding=True).to(device)
            out = model(**tok).logits
            scores = torch.nn.functional.softmax(out, dim=1)
            for idx, score in enumerate(scores):
                if '●' in sentences[idx]:
                    score = np.array([1, 0])
                res = score.argmax()
                ret['pred'].append('부정' if res == 0 else '긍정')
                ret['score'].append(score[res].item())
    return ret
