from IndexSelection.Chain.Handler import Handler
from IndexSelection.Chain.PipelineResult import PipelineResult
from IndexSelection.Chain.Exceptions import FilterRejectException
import re

class QualityFilter(Handler):
    """
    Stage 2: 品質過濾 (Enhanced)
    
    [新增至 data._index_content 的資料]:
    - quality_status (str): 'OK', 'Rescued', or 'Low'
    - is_hub_page (bool): 是否因為連結多而被保留的短頁面
    - quality_score_ttr (float): 詞彙豐富度 (0.0 - 1.0)
    """
    
    # 定義 Soft 404 關鍵字字典
    SOFT_404_MAP = {
        'en': ['not found', 'error 404', 'page unavailable', 'does not exist', 'back to home'],
        'zh-tw': ['找不到頁面', '查無此人', '商品已下架', '404 錯誤', '頁面不存在'],
        'zh-cn': ['页面未找到', '404 错误', '访问的页面不存在'],
        'de': ['seite nicht gefunden', 'fehler 404', 'nicht verfügbar'],
        'ja': ['ページが見つかりません', '存在しません'],
        'default': ['404', 'not found', 'error'] # 通用備案
    }

    def __init__(self):
        super().__init__()
        self.name = "QualityFilter"
        self.processors = [
            self._check_length_and_rescue,  # 1. 長度與權威救援
            self._check_soft_404_multilang, # 2. 多語言 Soft 404
            self._check_information_density,# 3. TTR 計算 (SEO Spam)
            self._check_parser_artifacts    # 4. [新增] 檢查是否抓到程式碼
        ]

    def canHandle(self, data):
        # 確保有 content_length，且 content 本身也要在
        return 'content_length' in data._index_content and 'content' in data._index_content

    def handle(self, data) -> PipelineResult:
        if not self.canHandle(data):
            return PipelineResult(success=False, stage=self.name, reason="Missing content")

        try:
            for processor in self.processors:
                processor(data)

        except FilterRejectException as e:
            return PipelineResult(success=False, stage=self.name, reason=str(e))
        except Exception as e:
            return PipelineResult(success=False, stage=self.name, reason=f"Error: {str(e)}")

        return super().handle(data)

    # --- Processors ---

    def _check_length_and_rescue(self, data):
        """
        檢查長度，但針對高權重 (Inlinks) 的頁面給予豁免 (Hub Page)
        """
        length = data._index_content['content_length']
        inlinks = data.inlink_count if hasattr(data, 'inlink_count') else 0
        
        # 基礎門檻：50字
        if length < 50:
            # Authority Rescue: 如果連結數 > 100，視為重要導航頁，予以保留
            if inlinks > 100:
                data._index_content['quality_status'] = 'Rescued'
                data._index_content['is_hub_page'] = True
                return # 放行，不繼續往下檢查 TTR (短文 TTR 不準)
            else:
                raise FilterRejectException(f"Content too short")
        
        data._index_content['quality_status'] = 'OK'
        data._index_content['is_hub_page'] = False

    def _check_soft_404_multilang(self, data):
        """
        多語言 Soft 404 偵測 (全語言掃描版)
        策略：不管這頁面是什麼語言，用所有已知的 404 關鍵字都掃一遍。
        """
        content = data._index_content.get('content', '').lower()
        
        # 1. 前置過濾：只有內容很短時才檢查
        # 如果內容很長，即使標題有 "Not Found" 也可能是技術文章，所以跳過
        if len(content) >= 200:
            return

        title = data._index_content.get('title', '').lower()
        
        # 2. 扁平化關鍵字列表 (Flattening)
        # 將 map 中所有語言的 list 合併成一個大的 set，避免重複檢查
        # 為了效能，其實這一步最好放在 __init__ 做一次就好，但寫在這裡比較好理解
        all_keywords = set()
        for lang_list in self.SOFT_404_MAP.values():
            all_keywords.update(lang_list)
            
        # 3. 掃描所有關鍵字
        for k in all_keywords:
            # 檢查標題
            if k in title:
                # 找到就直接報錯，不需要繼續找了
                raise FilterRejectException(f"Soft 404")
            
            # 檢查內容開頭 (前 100 字)
            if k in content[:100]:
                raise FilterRejectException(f"Soft 404")

    def _check_information_density(self, data):
        """
        計算 TTR (Type-Token Ratio) = 獨特詞數 / 總詞數
        用來過濾 Keyword Stuffing (關鍵字堆砌)
        """
        # 如果是 Hub Page (已在長度檢查被 Rescue)，通常字很少，跳過 TTR 檢查
        if data._index_content.get('is_hub_page'):
            return

        content = data._index_content.get('content', '')
        
        # 簡單斷詞 (通用版)
        words = re.split(r'\W+', content)
        words = [w for w in words if w.strip() and len(w) > 1] # 過濾掉單字元
        
        total_words = len(words)
        if total_words == 0:
            # 如果分不出詞，可能是全符號，視為垃圾
            raise FilterRejectException("No valid words found in content")
            
        unique_words = len(set(words))
        
        ttr = unique_words / total_words
        data._index_content['quality_score_ttr'] = round(ttr, 4)
        
        # 閾值判斷：
        # TTR < 0.15: 極度重複 (e.g. "鞋子 便宜鞋子 買鞋子 鞋子...") -> SEO Spam
        # TTR > 0.95 (且文章很長): 可能也是亂碼或 Hash 列表
        
        if total_words > 100 and ttr < 0.15:
            raise FilterRejectException(f"Low TTR")

    def _check_parser_artifacts(self, data):
        """
        [新增] 檢查解析殘留 (Code/JSON Artifacts)
        有時候爬蟲會把 JS 代碼或 JSON 當成內文抓下來。
        """
        content = data._index_content.get('content', '')
        if len(content) < 100: 
            return # 短文不檢查這個，避免誤判
            
        # 計算特徵符號的密度
        # JS/JSON 常見符號: { } ; " [ ]
        code_symbols = len(re.findall(r'[\{\}\;\"\[\]]', content))
        symbol_ratio = code_symbols / len(content)
        
        # 如果超過 10% 的內容是這些符號，極高機率是 Code
        if symbol_ratio > 0.1:
            raise FilterRejectException(f"High density of code symbols ({symbol_ratio:.2%}). Likely Parse Error.")
            
        # 關鍵字檢查：如果是 JSON 回傳，通常開頭會有 {"status":...
        if content.strip().startswith(('{"', "['", 'function(')):
            raise FilterRejectException("Content looks like Raw JSON or JS Code")