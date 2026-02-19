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

    def __str__(self) -> str:
        # The main error message is already set in __init__, just add SQL context if helpful
        msg = super().__str__()

        # Optionally show a brief SQL snippet for context
        if self.sql and len(self.sql) > 200:
            # Only show snippet if SQL is long (short SQL is fine to show in full)
            sql_lines = self.sql.strip().split('\n')
            first_line = sql_lines[0] if sql_lines else self.sql
            sql_snippet = first_line[:100] + "..." if len(first_line) > 100 else first_line
            msg += f"\n\nSQL operation: {sql_snippet}"

        return msg
