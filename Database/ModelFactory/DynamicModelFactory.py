class DynamicModelFactory:

    def __init__(self, base):
        self._base = base
        self._cache = {}

    def get_or_create(self, mixin, table_base_name: str, class_base_name: str, suffix: str):
        """
        核心工廠方法：取得已存在的 Class 或建立新的。
        """
        # snake_case for table: crawler_stat_total
        table_name = f"{table_base_name}_{suffix.lower()}"
        
        # PascalCase for class: CrawlerStatTotal
        class_name = f"{class_base_name}{suffix}"

        if table_name in self._cache:
            return self._cache[table_name]

        # type(name, bases, attrs)
        model_class = type(
            class_name,
            (mixin, self._base),
            {
                "__tablename__": table_name,
                # 如果有需要，也可以在這裡動態加入 __table_args__
                # "__table_args__": {"extend_existing": True} 
            }
        )

        # 4. 存入快取並回傳
        self._cache[table_name] = model_class
        return model_class