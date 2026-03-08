import re
from dictionaries import ANTONYMS, CRITICAL_MODIFIERS

def is_safe_merge(word1, word2):
    w1, w2 = str(word1).lower(), str(word2).lower()
    
    if len(w1) <= 4 or len(w2) <= 4:
        if w1 not in w2 and w2 not in w1:
            return False

    nums1 = set(re.findall(r'\d+', w1))
    nums2 = set(re.findall(r'\d+', w2))
    if nums1 and nums2 and nums1 != nums2:
        return False
        
    for set1, set2 in ANTONYMS:
        if (any(a in w1 for a in set1) and any(b in w2 for b in set2)) or \
           (any(a in w2 for a in set1) and any(b in w1 for b in set2)):
            return False

    for mod in CRITICAL_MODIFIERS:
        in_w1 = mod in w1
        in_w2 = mod in w2
        if in_w1 != in_w2:
            return False
            
    return True

def calculate_penalty(item):
    item_id, text_val = item
    penalty = 0
    
    if not bool(re.search('[а-яА-ЯёЁ]', text_val)): 
        penalty += 1000

    if text_val.isupper() and len(text_val) > 1: 
        penalty += 100

    penalty += len(text_val)

    penalty += item_id * 0.0001
    return penalty
