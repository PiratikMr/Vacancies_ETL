import torch
from sentence_transformers import SentenceTransformer, util
from rapidfuzz import fuzz
import config
from rich.console import Console

console = Console()

class HybridMatcher:
    def __init__(self):
        with console.status(f"[cyan]Загрузка семантической модели '{config.SEMANTIC_MODEL_NAME}'..."):
            self.model = SentenceTransformer(config.SEMANTIC_MODEL_NAME)
        self.golden_embeddings = None
        self.golden_names = []
        self.golden_ids = []
    
    def prepare_golden(self, golden_records):
        if not golden_records:
            console.print("[yellow]Эталонные записи не найдены.")
            return
            
        self.golden_ids = [r[0] for r in golden_records]
        self.golden_names = [r[1] for r in golden_records]
        
        texts = [name.lower() for name in self.golden_names]
        with console.status(f"[cyan]Вычисление семантических эмбеддингов для {len(texts)} эталонных записей..."):
            self.golden_embeddings = self.model.encode(texts, convert_to_tensor=True)
        console.print(f"[green]Успешно обработано {len(texts)} эталонных записей.")

    def find_best_matches(self, cand_name, threshold):
        if self.golden_embeddings is None or not self.golden_names:
            return []
            
        cand_embed = self.model.encode([cand_name.lower()], convert_to_tensor=True)
        
        cosine_scores = util.cos_sim(cand_embed, self.golden_embeddings)[0]
        
        matches = []
        for i, sem_score_tensor in enumerate(cosine_scores):
            sem_score = sem_score_tensor.item()
            golden_name = self.golden_names[i]
            golden_id = self.golden_ids[i]
            
            c_lower = cand_name.lower()
            g_lower = golden_name.lower()
            len_c = len(c_lower)
            len_g = len(g_lower)
            
            trans_map = str.maketrans("соаерхум", "coaepxym")
            c_norm = c_lower.translate(trans_map)
            g_norm = g_lower.translate(trans_map)
            
            rf_score = fuzz.WRatio(c_norm, g_norm) / 100.0
            exact_ratio = fuzz.ratio(c_norm, g_norm) / 100.0
            
            final_score = (rf_score * config.RAPIDFUZZ_WEIGHT) + (sem_score * config.SEMANTIC_WEIGHT)
            
            if len_g <= 3 and len_c <= 3 and exact_ratio < 0.90:
                final_score = 0.0
                
            elif len_g <= 2 and len_c > len_g + 2:
                final_score = 0.0
            elif len_c <= 2 and len_g > len_c + 2:
                final_score = 0.0
                
            elif max(len_g, len_c) <= 6 and exact_ratio < 0.85:
                if sem_score < 0.92:
                    final_score = 0.0
            
            if final_score >= threshold:
                matches.append({
                    "golden_id": golden_id,
                    "golden_name": golden_name,
                    "final_score": final_score,
                    "sem_score": sem_score,
                    "fuzz_score": rf_score
                })
        
        matches.sort(key=lambda x: x['final_score'], reverse=True)
        return matches
