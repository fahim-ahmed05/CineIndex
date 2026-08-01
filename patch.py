with open('app/main.py', 'r', encoding='utf-8') as f:
    text = f.read()

import re

# 1. Remove max_per_root from DEFAULT_CONFIG
text = re.sub(r'\s*\"max_per_root\": 0,', '', text)

# 2 & 3. Replace from _tree_path_parts to end of _crawl_root_with_tree
pattern_tree = re.compile(r'def _tree_path_parts\(.*?(def _crawl_root_with_tree\(.*?return \([\s\S]*?,\n    \))', re.DOTALL)
new_crawl = '''def _crawl_and_count(
    rc,
    *,
    crawl_cfg: dict,
    conn,
    incremental: bool,
) -> tuple[int, int, int, int, float]:
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM dirs WHERE root = ?", (rc.url,))
    before_dirs = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM media WHERE root = ?", (rc.url,))
    before_media = cur.fetchone()[0]

    result = crawl_root(
        rc,
        crawl_cfg,
        conn=conn,
        incremental=incremental,
        summary_only=True,
    )

    cur.execute("SELECT COUNT(*) FROM dirs WHERE root = ?", (rc.url,))
    after_dirs = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM media WHERE root = ?", (rc.url,))
    after_media = cur.fetchone()[0]

    return (
        before_dirs,
        after_dirs,
        before_media,
        after_media,
        result.elapsed_seconds,
    )'''
text = pattern_tree.sub(new_crawl, text)

# 4. Modify main loop
text = re.sub(r'\s*try:\s*max_per_root = int\(cfg_raw\.get\(\"max_per_root\", 0\) or 0\)\s*except Exception:\s*max_per_root = 0', '', text)
text = re.sub(r'\s*root_tag_map = build_root_tag_map\(\)\n\s*for index, rc in enumerate\(crawl_targets, start=1\):', r'\n        for index, rc in enumerate(crawl_targets, start=1):', text)

old_crawl_call = '''            (
                before_dirs,
                after_dirs,
                before_media,
                after_media,
                elapsed_seconds,
                suppressed,
            ) = _crawl_root_with_tree(
                rc,
                crawl_cfg=crawl_cfg,
                conn=conn,
                root_tag_map=root_tag_map,
                incremental=incremental,
                max_per_root=max_per_root,
            )'''

new_crawl_call = '''            print(Fore.CYAN + f"[{action_name.upper()}] {index}/{total_roots} Indexing | root={rc.url} ", end="\\r", flush=True)
            (
                before_dirs,
                after_dirs,
                before_media,
                after_media,
                elapsed_seconds,
            ) = _crawl_and_count(
                rc,
                crawl_cfg=crawl_cfg,
                conn=conn,
                incremental=incremental,
            )'''
text = text.replace(old_crawl_call, new_crawl_call)

text = re.sub(r'\s*if suppressed > 0:\s*print\(Fore\.YELLOW \+ f\"  \.\.\. \+\{suppressed\} more omitted for this root\"\)', '', text)

with open('app/main.py', 'w', encoding='utf-8') as f:
    f.write(text)
print('Done!')
