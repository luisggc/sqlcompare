class DBConnectionError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        conn_id: str,
        sql: str | None = None,
        original: Exception | None = None,
    ):
        super().__init__(message)
        self.conn_id = conn_id
        self.sql = sql
        self.original = original
