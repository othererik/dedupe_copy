from dedupe_copy.disk_cache_dict import CacheDict
import os

cd = CacheDict(db_file="test.db")
cd["a"] = None
cd.save()
assert "a" in cd
print("CacheDict works")
os.unlink("test.db")
