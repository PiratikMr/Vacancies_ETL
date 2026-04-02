import sys
from rich.console import Console
from rich.table import Table
from rich.prompt import Prompt

import config
import db_ops
from matcher import HybridMatcher

console = Console()

def run_pipeline(dimension_name: str):
    if dimension_name not in config.DIMENSIONS_CONFIG:
        console.print(f"[red]Ошибка: Измерение '{dimension_name}' не определено в config.py.[/red]")
        sys.exit(1)
        
    dim_conf = config.DIMENSIONS_CONFIG[dimension_name]
    threshold = dim_conf['threshold']
    relation_type = dim_conf['relation_type']
    
    console.print(f"\n[bold green]Запуск нормализации для измерения '{dimension_name}'[/bold green]")
    console.print(f"[dim]Порог: {threshold} | Тип связи: {relation_type}[/dim]\n")
    
    with console.status("[cyan]Получение записей из базы данных..."):
        golden_records = db_ops.get_records(dimension_name, is_reference=True)
        candidate_records = db_ops.get_records(dimension_name, is_reference=False)
        
    console.print(f"Эталонные записи: [bold green]{len(golden_records)}[/bold green]")
    console.print(f"Записи-кандидаты: [bold yellow]{len(candidate_records)}[/bold yellow]\n")
    
    if not golden_records or not candidate_records:
        console.print("[yellow]Недостаточно данных для выполнения сопоставления. Выход.[/yellow]")
        return

    matcher = HybridMatcher()
    matcher.prepare_golden(golden_records)
    
    proposed_merges = []
    
    with console.status(f"[cyan]Сопоставление {len(candidate_records)} кандидатов с эталонным набором данных..."):
        for cand_id, cand_name in candidate_records:
            matches = matcher.find_best_matches(cand_name, threshold=threshold)
            if matches:
                 best = matches[0]
                 proposed_merges.append({
                     "candidate_id": cand_id,
                     "candidate_name": cand_name,
                     "golden_id": best['golden_id'],
                     "golden_name": best['golden_name'],
                     "score": best['final_score'],
                     "sem_score": best['sem_score'],
                     "fuzz_score": best['fuzz_score']
                 })
                 
    if not proposed_merges:
        console.print("[green]Ни один кандидат не преодолел порог. Всё выглядит чисто![/green]")
        return
        
    grouped_merges = {}
    for m in proposed_merges:
        g = m['golden_name']
        if g not in grouped_merges:
            grouped_merges[g] = []
        grouped_merges[g].append(m['candidate_name'])
        
    console.print("\n[bold]Предложенные объединения:[/bold]\n")
    for g_name, c_names in grouped_merges.items():
        console.print(f"✅ Эталон: [bold green]{g_name}[/bold green]")
        for c_name in c_names:
            console.print(f"   ↳ [magenta]{c_name}[/magenta]")
        console.print("-" * 30)
    
    choice = Prompt.ask("\nПрименить ВСЕ эти объединения к базе данных? (y/n)", choices=["y", "n"], default="n").strip().lower()
    
    if choice != 'y':
        console.print("[yellow]Отменено пользователем. База данных НЕ изменена.[/yellow]")
        return
        
    final_merges = proposed_merges

    if not final_merges:
        console.print("[yellow]Нет подходящих совпадений для объединения. Выход.[/yellow]")
        return
        
    with console.status(f"[bold red]Применение {len(final_merges)} объединений в PostgreSQL..."):
        for m in final_merges:
            db_ops.apply_normalization(
                dimension_name=dimension_name,
                relation_type=relation_type,
                candidate_id=m['candidate_id'],
                golden_id=m['golden_id']
            )
            
    console.print(f"[bold green]Успешно применено {len(final_merges)} объединений для измерения '{dimension_name}'![/bold green]")
    console.print("Нормализация завершена.")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Базовая нормализация NLP")
    parser.add_argument("--dim", type=str, required=True, help="Измерение для нормализации (например, skill, employer)")
    
    args = parser.parse_args()
    run_pipeline(args.dim)
