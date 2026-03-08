from sentence_transformers import SentenceTransformer
from sklearn.cluster import AgglomerativeClustering
from sklearn.metrics.pairwise import cosine_distances

def load_model(model_name):
    print(f"\n[Система] Загружаем нейросеть ({model_name})...")
    return SentenceTransformer(model_name)

def get_embeddings(model, words, prefix=''):
    words_for_nn = [f"{prefix}{str(w)}" for w in words]
    return model.encode(words_for_nn, show_progress_bar=False)

def cluster_words(embeddings, threshold):
    max_distance = 1.0 - threshold
    distances = cosine_distances(embeddings)
    
    clustering = AgglomerativeClustering(
        n_clusters=None, 
        metric='precomputed', 
        linkage='complete', 
        distance_threshold=max_distance
    )
    clustering.fit(distances)
    return clustering.labels_
