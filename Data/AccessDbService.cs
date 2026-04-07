using System.Data.OleDb;
using System.Data.Common;
using System.Data;
using System.Globalization;
using практика_2._0.Models;

namespace практика_2._0.Data;

public sealed class AccessDbService
{
    private readonly string _connectionString;
    private static readonly SemaphoreSlim _schemaLock = new(1, 1);
    private static volatile bool _userModerationSchemaEnsured;
    private static volatile bool _courseTeacherSchemaEnsured;
    private static volatile bool _homeworkReviewSchemaEnsured;
    private static volatile bool _teacherCoursesSchemaEnsured;
    private static volatile bool _enrollmentStatusColumnEnsured;
    private static volatile bool _userRubBalanceColumnEnsured;
    private static volatile bool _enrollmentRefundProcessedColumnEnsured;

    /// <summary>Совпадает с логикой <see cref="SetEnrollmentStatusAsync"/> — в разных БД колонка называется по-разному.</summary>
    private static string GetEnrollmentCourseColumn(OleDbConnection connection, string enrollmentTable) =>
        GetExistingColumnName(connection, enrollmentTable, "ID_cours", "ID_curs");

    /// <summary>Совпадает с логикой <see cref="SetEnrollmentStatusAsync"/>.</summary>
    private static string? TryGetEnrollmentStatusColumn(OleDbConnection connection, string enrollmentTable) =>
        TryGetExistingColumnName(connection, enrollmentTable, "status", "State", "enrollment_status", "learning_status");

    private static IReadOnlyList<string> GetEnrollmentStatusColumns(OleDbConnection connection, string enrollmentTable)
    {
        // В Access иногда встречаются колонки со скрытыми пробелами в имени (например "status ").
        // Поэтому берём имена колонок через схему и сравниваем по Trim(). Обновляем все найденные варианты.
        var cols = new List<string>(capacity: 8);

        var expected = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "status",
            "state",
            "enrollment_status",
            "learning_status"
        };

        try
        {
            var schema = connection.GetSchema("Columns");
            foreach (DataRow row in schema.Rows)
            {
                var tableName = row["TABLE_NAME"]?.ToString();
                if (!string.Equals(tableName, enrollmentTable, StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                var raw = row["COLUMN_NAME"]?.ToString();
                if (string.IsNullOrWhiteSpace(raw))
                {
                    continue;
                }

                var trimmed = raw.Trim();
                // строго ожидаемые имена + любые варианты, содержащие "status"
                if (expected.Contains(trimmed) || trimmed.Contains("status", StringComparison.OrdinalIgnoreCase))
                {
                    if (!cols.Contains(raw, StringComparer.OrdinalIgnoreCase))
                    {
                        cols.Add(raw);
                    }
                }
            }
        }
        catch
        {
            // fallback на старую схему поиска по именам
            var c1 = TryGetExistingColumnName(connection, enrollmentTable, "status", "State", "enrollment_status", "learning_status");
            if (!string.IsNullOrWhiteSpace(c1))
            {
                cols.Add(c1);
            }
        }

        return cols;
    }

    private static string? TryGetEnrollmentRefundProcessedColumn(OleDbConnection connection, string enrollmentTable) =>
        TryGetExistingColumnName(connection, enrollmentTable, "refund_processed", "is_refund_processed", "refund_done");

    private static string? TryGetEnrollmentRefundAmountColumn(OleDbConnection connection, string enrollmentTable) =>
        TryGetExistingColumnName(connection, enrollmentTable, "refund_amount", "refund_sum", "refund_rub");

    public async Task EnsureEnrollmentRefundColumnsAsync(CancellationToken cancellationToken = default)
    {
        if (_enrollmentRefundProcessedColumnEnsured)
        {
            return;
        }

        await _schemaLock.WaitAsync(CancellationToken.None);
        try
        {
            if (_enrollmentRefundProcessedColumnEnsured)
            {
                return;
            }

            await using var connection = new OleDbConnection(_connectionString);
            await connection.OpenAsync(CancellationToken.None);
            var enrollmentTable = GetExistingTableName(connection, "ENROLLMENTS");

            var processedCol = TryGetEnrollmentRefundProcessedColumn(connection, enrollmentTable);
            if (processedCol is null)
            {
                await using var alter = connection.CreateCommand();
                alter.CommandText = $"ALTER TABLE [{enrollmentTable}] ADD COLUMN [refund_processed] YESNO";
                try
                {
                    await alter.ExecuteNonQueryAsync(CancellationToken.None);
                    await connection.CloseAsync();
                    await connection.OpenAsync(CancellationToken.None);
                }
                catch (OleDbException)
                {
                }
            }

            var amountCol = TryGetEnrollmentRefundAmountColumn(connection, enrollmentTable);
            if (amountCol is null)
            {
                await using var alter2 = connection.CreateCommand();
                alter2.CommandText = $"ALTER TABLE [{enrollmentTable}] ADD COLUMN [refund_amount] DOUBLE";
                try
                {
                    await alter2.ExecuteNonQueryAsync(CancellationToken.None);
                    await connection.CloseAsync();
                    await connection.OpenAsync(CancellationToken.None);
                }
                catch (OleDbException)
                {
                }
            }

            _enrollmentRefundProcessedColumnEnsured = TryGetEnrollmentRefundProcessedColumn(connection, enrollmentTable) is not null;
        }
        finally
        {
            _schemaLock.Release();
        }
    }

    private static string? TryGetUserRubBalanceColumn(OleDbConnection connection, string userTable) =>
        TryGetExistingColumnName(connection, userTable, "balance_rub", "refund_balance_rub", "rub_balance");

    public async Task EnsureUserRubBalanceColumnAsync(CancellationToken cancellationToken = default)
    {
        if (_userRubBalanceColumnEnsured)
        {
            return;
        }

        await _schemaLock.WaitAsync(CancellationToken.None);
        try
        {
            if (_userRubBalanceColumnEnsured)
            {
                return;
            }

            await using var connection = new OleDbConnection(_connectionString);
            await connection.OpenAsync(CancellationToken.None);
            var userTable = GetExistingTableName(connection, "USER", "USERS");
            var rubCol = TryGetUserRubBalanceColumn(connection, userTable);
            if (rubCol is null)
            {
                OleDbException? last = null;
                // Пытаемся добавить под основным именем, затем под альтернативным.
                foreach (var colName in new[] { "balance_rub", "refund_balance_rub" })
                {
                    await using var alter = connection.CreateCommand();
                    alter.CommandText = $"ALTER TABLE [{userTable}] ADD COLUMN [{colName}] DOUBLE";
                    try
                    {
                        await alter.ExecuteNonQueryAsync(CancellationToken.None);
                        await connection.CloseAsync();
                        await connection.OpenAsync(CancellationToken.None);
                        rubCol = TryGetUserRubBalanceColumn(connection, userTable);
                        if (rubCol is not null)
                        {
                            break;
                        }
                    }
                    catch (OleDbException ex)
                    {
                        last = ex;
                    }
                }

                if (rubCol is null)
                {
                    throw new InvalidOperationException(
                        $"Не удалось добавить колонку рублёвого баланса в таблицу [{userTable}] (balance_rub/refund_balance_rub). " +
                        $"Access сообщил: {last?.Message ?? "неизвестная ошибка"}");
                }
            }

            _userRubBalanceColumnEnsured = TryGetUserRubBalanceColumn(connection, userTable) is not null;
        }
        finally
        {
            _schemaLock.Release();
        }
    }

    public AccessDbService(IConfiguration configuration)
    {
        _connectionString = configuration.GetConnectionString("AccessConnection")
            ?? throw new InvalidOperationException("Connection string 'AccessConnection' is not configured.");
    }

    private async Task EnsureHomeworkReviewTableAsync(CancellationToken cancellationToken = default)
    {
        if (_homeworkReviewSchemaEnsured)
        {
            return;
        }

        // Миграции схемы не должны падать из-за отмены HTTP-запроса.
        await _schemaLock.WaitAsync(CancellationToken.None);
        try
        {
            if (_homeworkReviewSchemaEnsured)
            {
                return;
            }

            await using var connection = new OleDbConnection(_connectionString);
            await connection.OpenAsync(CancellationToken.None);

            // Если таблица уже есть — просто считаем, что всё ок.
            var existing = TryGetExistingTableName(connection, "HOMEWORK_REVIEWS", "HW_REVIEWS", "HOMEWORK_REVIEW");
            if (existing is not null)
            {
                _homeworkReviewSchemaEnsured = true;
                return;
            }

            await using var cmd = connection.CreateCommand();
            cmd.CommandText = """
                CREATE TABLE HOMEWORK_REVIEWS (
                    ID_user INTEGER,
                    ID_cours INTEGER,
                    ID_lesson INTEGER,
                    ID_homework INTEGER,
                    status TEXT(32),
                    grade INTEGER,
                    submitted_at DATETIME,
                    reviewed_at DATETIME,
                    teacher_user_id INTEGER
                )
                """;
            await cmd.ExecuteNonQueryAsync(cancellationToken);

            _homeworkReviewSchemaEnsured = true;
        }
        finally
        {
            _schemaLock.Release();
        }
    }

    /// <summary>
    /// Имена ключевых столбцов в таблице проверок ДЗ (в разных БД могут отличаться).
    /// </summary>
    private static (string UserCol, string CourseCol, string LessonCol, string HwCol, string StatusCol, string GradeCol, string SubmittedAtCol, string ReviewedAtCol, string TeacherCol)
        ResolveHomeworkReviewColumnNames(OleDbConnection connection, string table)
    {
        var userCol = TryGetExistingColumnName(connection, table, "ID_user", "user_id", "UserId");
        var courseCol = TryGetExistingColumnName(connection, table, "ID_cours", "ID_curs", "course_id", "ID_course");
        var lessonCol = TryGetExistingColumnName(connection, table, "ID_lesson", "lesson_id", "ID_lessons");
        var hwCol = TryGetExistingColumnName(connection, table, "ID_homework", "homework_id", "ID_hw");
        var statusCol = TryGetExistingColumnName(connection, table, "status", "hw_status", "state");
        var gradeCol = TryGetExistingColumnName(connection, table, "grade", "mark", "score");
        var subAtCol = TryGetExistingColumnName(connection, table, "submitted_at", "submit_at", "sent_at");
        var revAtCol = TryGetExistingColumnName(connection, table, "reviewed_at", "check_at", "checked_at");
        var teacherCol = TryGetExistingColumnName(connection, table, "teacher_user_id", "teacher_id", "ID_teacher");

        if (userCol is null || courseCol is null || lessonCol is null || hwCol is null)
        {
            throw new InvalidOperationException(
                $"Таблица проверок ДЗ [{table}] должна содержать столбцы пользователя, курса, урока и ДЗ (ожидались ID_user / ID_cours / ID_lesson / ID_homework).");
        }

        statusCol ??= "status";
        gradeCol ??= "grade";
        subAtCol ??= "submitted_at";
        revAtCol ??= "reviewed_at";
        teacherCol ??= "teacher_user_id";

        return (userCol, courseCol, lessonCol, hwCol, statusCol, gradeCol, subAtCol, revAtCol, teacherCol);
    }

    /// <summary>
    /// В "грязных" Access-БД ID-колонки иногда текстовые и могут содержать мусор.
    /// IsNumeric защищает от "Type mismatch" в WHERE при CLng/сравнении.
    /// </summary>
    private static string HomeworkReviewIdExpr(string columnName) =>
        $"CLng(IIf(IsNumeric([{columnName}]), [{columnName}], 0))";

    public async Task UpsertHomeworkSubmittedAsync(
        int userId,
        int courseId,
        int lessonId,
        int homeworkId,
        DateTime submittedAt,
        CancellationToken cancellationToken = default)
    {
        await EnsureHomeworkReviewTableAsync(cancellationToken);

        await using var connection = new OleDbConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        var table = GetExistingTableName(connection, "HOMEWORK_REVIEWS", "HW_REVIEWS", "HOMEWORK_REVIEW");
        var cols = ResolveHomeworkReviewColumnNames(connection, table);

        await using (var update = connection.CreateCommand())
        {
            update.CommandText = $"""
                UPDATE [{table}]
                SET
                    [{cols.StatusCol}] = ?,
                    [{cols.SubmittedAtCol}] = ?,
                    [{cols.ReviewedAtCol}] = NULL,
                    [{cols.TeacherCol}] = NULL,
                    [{cols.GradeCol}] = NULL
                WHERE {HomeworkReviewIdExpr(cols.UserCol)} = ? AND {HomeworkReviewIdExpr(cols.HwCol)} = ?
                """;
            update.Parameters.Add(new OleDbParameter("@p1", OleDbType.VarWChar) { Value = "submitted" });
            update.Parameters.Add(new OleDbParameter("@p2", OleDbType.Date) { Value = submittedAt });
            update.Parameters.Add(new OleDbParameter("@p3", OleDbType.Integer) { Value = userId });
            update.Parameters.Add(new OleDbParameter("@p4", OleDbType.Integer) { Value = homeworkId });

            var affected = await update.ExecuteNonQueryAsync(cancellationToken);
            if (affected > 0)
            {
                return;
            }
        }

        try
        {
            await using var insert = connection.CreateCommand();
            insert.CommandText = $"""
                INSERT INTO [{table}]
                    ([{cols.UserCol}], [{cols.CourseCol}], [{cols.LessonCol}], [{cols.HwCol}], [{cols.StatusCol}], [{cols.GradeCol}], [{cols.SubmittedAtCol}], [{cols.ReviewedAtCol}], [{cols.TeacherCol}])
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """;
            insert.Parameters.Add(new OleDbParameter("@p1", OleDbType.Integer) { Value = userId });
            insert.Parameters.Add(new OleDbParameter("@p2", OleDbType.Integer) { Value = courseId });
            insert.Parameters.Add(new OleDbParameter("@p3", OleDbType.Integer) { Value = lessonId });
            insert.Parameters.Add(new OleDbParameter("@p4", OleDbType.Integer) { Value = homeworkId });
            insert.Parameters.Add(new OleDbParameter("@p5", OleDbType.VarWChar) { Value = "submitted" });
            insert.Parameters.Add(new OleDbParameter("@p6", OleDbType.Integer) { Value = DBNull.Value });
            insert.Parameters.Add(new OleDbParameter("@p7", OleDbType.Date) { Value = submittedAt });
            insert.Parameters.Add(new OleDbParameter("@p8", OleDbType.Date) { Value = DBNull.Value });
            insert.Parameters.Add(new OleDbParameter("@p9", OleDbType.Integer) { Value = DBNull.Value });
            _ = await insert.ExecuteNonQueryAsync(cancellationToken);
        }
        catch (OleDbException)
        {
            // Fallback для старых схем: вставляем только обязательные поля.
            await using var fallback = connection.CreateCommand();
            fallback.CommandText = $"""
                INSERT INTO [{table}]
                    ([{cols.UserCol}], [{cols.CourseCol}], [{cols.LessonCol}], [{cols.HwCol}], [{cols.StatusCol}], [{cols.SubmittedAtCol}])
                VALUES (?, ?, ?, ?, ?, ?)
                """;
            fallback.Parameters.Add(new OleDbParameter("@p1", OleDbType.Integer) { Value = userId });
            fallback.Parameters.Add(new OleDbParameter("@p2", OleDbType.Integer) { Value = courseId });
            fallback.Parameters.Add(new OleDbParameter("@p3", OleDbType.Integer) { Value = lessonId });
            fallback.Parameters.Add(new OleDbParameter("@p4", OleDbType.Integer) { Value = homeworkId });
            fallback.Parameters.Add(new OleDbParameter("@p5", OleDbType.VarWChar) { Value = "submitted" });
            fallback.Parameters.Add(new OleDbParameter("@p6", OleDbType.Date) { Value = submittedAt });
            _ = await fallback.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    public async Task<HomeworkReviewItem?> GetHomeworkReviewAsync(
        int userId,
        int homeworkId,
        CancellationToken cancellationToken = default)
    {
        await EnsureHomeworkReviewTableAsync(cancellationToken);

        await using var connection = new OleDbConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        var table = GetExistingTableName(connection, "HOMEWORK_REVIEWS", "HW_REVIEWS", "HOMEWORK_REVIEW");
        var cols = ResolveHomeworkReviewColumnNames(connection, table);

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $"""
            SELECT TOP 1
                [{cols.UserCol}] AS rid_user,
                [{cols.CourseCol}] AS rid_course,
                [{cols.LessonCol}] AS rid_lesson,
                [{cols.HwCol}] AS rid_hw,
                [{cols.StatusCol}] AS rid_status,
                [{cols.GradeCol}] AS rid_grade,
                [{cols.SubmittedAtCol}] AS rid_submitted_at,
                [{cols.ReviewedAtCol}] AS rid_reviewed_at,
                [{cols.TeacherCol}] AS rid_teacher
            FROM [{table}]
            WHERE {HomeworkReviewIdExpr(cols.UserCol)} = ? AND {HomeworkReviewIdExpr(cols.HwCol)} = ?
            ORDER BY [{cols.ReviewedAtCol}] DESC, [{cols.SubmittedAtCol}] DESC
            """;
        cmd.Parameters.AddWithValue("@p1", userId);
        cmd.Parameters.AddWithValue("@p2", homeworkId);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        if (reader is null || !await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return new HomeworkReviewItem
        {
            UserId = ToInt(reader["rid_user"]),
            CourseId = ToInt(reader["rid_course"]),
            LessonId = ToInt(reader["rid_lesson"]),
            HomeworkId = ToInt(reader["rid_hw"]),
            Status = ToStringSafe(reader["rid_status"]),
            Grade = ToNullableInt(reader["rid_grade"]),
            SubmittedAt = ToNullableDateTime(reader["rid_submitted_at"]),
            ReviewedAt = ToNullableDateTime(reader["rid_reviewed_at"]),
            TeacherUserId = ToNullableInt(reader["rid_teacher"])
        };
    }

    public async Task SetHomeworkReviewAsync(
        int teacherUserId,
        int userId,
        int courseId,
        int lessonId,
        int homeworkId,
        string status,
        int? grade,
        DateTime reviewedAt,
        CancellationToken cancellationToken = default)
    {
        await EnsureHomeworkReviewTableAsync(cancellationToken);

        await using var connection = new OleDbConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        var table = GetExistingTableName(connection, "HOMEWORK_REVIEWS", "HW_REVIEWS", "HOMEWORK_REVIEW");
        var cols = ResolveHomeworkReviewColumnNames(connection, table);

        await using (var update = connection.CreateCommand())
        {
            update.CommandText = $"""
                UPDATE [{table}]
                SET
                    [{cols.StatusCol}] = ?,
                    [{cols.GradeCol}] = ?,
                    [{cols.ReviewedAtCol}] = ?,
                    [{cols.TeacherCol}] = ?
                WHERE {HomeworkReviewIdExpr(cols.UserCol)} = ? AND {HomeworkReviewIdExpr(cols.HwCol)} = ?
                """;
            update.Parameters.AddWithValue("@p1", status);
            update.Parameters.AddWithValue("@p2", grade.HasValue ? grade.Value : (object)DBNull.Value);
            update.Parameters.Add(new OleDbParameter("@p3", OleDbType.Date) { Value = reviewedAt });
            update.Parameters.AddWithValue("@p4", teacherUserId);
            update.Parameters.AddWithValue("@p5", userId);
            update.Parameters.AddWithValue("@p6", homeworkId);

            var affected = await update.ExecuteNonQueryAsync(cancellationToken);
            // OleDb/Access иногда возвращает -1 при успешном UPDATE
            if (affected is -1 or > 0)
            {
                return;
            }
        }

        await using var insert = connection.CreateCommand();
        insert.CommandText = $"""
            INSERT INTO [{table}]
                ([{cols.UserCol}], [{cols.CourseCol}], [{cols.LessonCol}], [{cols.HwCol}], [{cols.StatusCol}], [{cols.GradeCol}], [{cols.SubmittedAtCol}], [{cols.ReviewedAtCol}], [{cols.TeacherCol}])
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;
        insert.Parameters.AddWithValue("@p1", userId);
        insert.Parameters.AddWithValue("@p2", courseId);
        insert.Parameters.AddWithValue("@p3", lessonId);
        insert.Parameters.AddWithValue("@p4", homeworkId);
        insert.Parameters.AddWithValue("@p5", status);
        insert.Parameters.AddWithValue("@p6", grade.HasValue ? grade.Value : (object)DBNull.Value);
        // Если submitted_at обязателен в конкретной схеме — ставим reviewedAt как минимум.
        insert.Parameters.Add(new OleDbParameter("@p7", OleDbType.Date) { Value = reviewedAt });
        insert.Parameters.Add(new OleDbParameter("@p8", OleDbType.Date) { Value = reviewedAt });
        insert.Parameters.AddWithValue("@p9", teacherUserId);
        _ = await insert.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<IReadOnlyList<HomeworkReviewItem>> GetHomeworkReviewsByUserIdAsync(
        int userId,
        CancellationToken cancellationToken = default)
    {
        await EnsureHomeworkReviewTableAsync(cancellationToken);

        var result = new List<HomeworkReviewItem>();
        await using var connection = new OleDbConnection(_connectionString);
        await connection.OpenAsync(CancellationToken.None);
        var table = GetExistingTableName(connection, "HOMEWORK_REVIEWS", "HW_REVIEWS", "HOMEWORK_REVIEW");
        var cols = ResolveHomeworkReviewColumnNames(connection, table);

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $"""
            SELECT
                [{cols.UserCol}] AS rid_user,
                [{cols.CourseCol}] AS rid_course,
                [{cols.LessonCol}] AS rid_lesson,
                [{cols.HwCol}] AS rid_hw,
                [{cols.StatusCol}] AS rid_status,
                [{cols.GradeCol}] AS rid_grade,
                [{cols.SubmittedAtCol}] AS rid_submitted_at,
                [{cols.ReviewedAtCol}] AS rid_reviewed_at,
                [{cols.TeacherCol}] AS rid_teacher
            FROM [{table}]
            WHERE {HomeworkReviewIdExpr(cols.UserCol)} = ?
            ORDER BY [{cols.SubmittedAtCol}] DESC
            """;
        cmd.Parameters.AddWithValue("@p1", userId);

        await using var reader = await cmd.ExecuteReaderAsync(CancellationToken.None);
        if (reader is null)
        {
            return result;
        }

        while (await reader.ReadAsync(CancellationToken.None))
        {
            result.Add(new HomeworkReviewItem
            {
                UserId = ToInt(reader["rid_user"]),
                CourseId = ToInt(reader["rid_course"]),
                LessonId = ToInt(reader["rid_lesson"]),
                HomeworkId = ToInt(reader["rid_hw"]),
                Status = ToStringSafe(reader["rid_status"]),
                Grade = ToNullableInt(reader["rid_grade"]),
                SubmittedAt = ToNullableDateTime(reader["rid_submitted_at"]),
                ReviewedAt = ToNullableDateTime(reader["rid_reviewed_at"]),
                TeacherUserId = ToNullableInt(reader["rid_teacher"])
            });
        }

        return result;
    }

    public async Task<int> GetCourseProgressPercentForUserAsync(
        int userId,
        int courseId,
        CancellationToken cancellationToken = default)
    {
        if (userId <= 0 || courseId <= 0)
        {
            return 0;
        }

        var lessons = await GetLessonsByCourseIdAsync(courseId, cancellationToken);
        if (lessons.Count == 0)
        {
            return 0;
        }

        // Собираем список домашек и их "вес" (Progres_bal).
        var homeworkMeta = new List<(int HomeworkId, int Weight)>();
        foreach (var l in lessons)
        {
            if (l.ID_lesson <= 0)
            {
                continue;
            }

            var hw = await GetProgressByLessonIdAsync(l.ID_lesson, cancellationToken);
            var hwId = hw?.ID_homework ?? 0;
            if (hwId <= 0)
            {
                continue;
            }

            var w = hw?.Progres_bal ?? 0;
            if (w <= 0)
            {
                w = 1;
            }

            homeworkMeta.Add((hwId, w));
        }

        if (homeworkMeta.Count == 0)
        {
            return 0;
        }

        var total = homeworkMeta.Sum(x => x.Weight);
        if (total <= 0)
        {
            return 0;
        }

        // Вытягиваем статусы по этим homeworkId.
        await EnsureHomeworkReviewTableAsync(cancellationToken);
        var accepted = new HashSet<int>();
        await using (var connection = new OleDbConnection(_connectionString))
        {
            await connection.OpenAsync(cancellationToken);
            var table = GetExistingTableName(connection, "HOMEWORK_REVIEWS", "HW_REVIEWS", "HOMEWORK_REVIEW");
            var cols = ResolveHomeworkReviewColumnNames(connection, table);

            foreach (var hwId in homeworkMeta.Select(x => x.HomeworkId).Distinct())
            {
                await using var cmd = connection.CreateCommand();
                cmd.CommandText = $"""
                    SELECT TOP 1 [{cols.StatusCol}] AS rid_status
                    FROM [{table}]
                    WHERE {HomeworkReviewIdExpr(cols.UserCol)} = ? AND {HomeworkReviewIdExpr(cols.HwCol)} = ?
                    ORDER BY [{cols.ReviewedAtCol}] DESC
                    """;
                cmd.Parameters.AddWithValue("@p1", userId);
                cmd.Parameters.AddWithValue("@p2", hwId);
                var statusObj = await cmd.ExecuteScalarAsync(cancellationToken);
                var status = ToStringSafe(statusObj);
                if (string.Equals(status, "accepted", StringComparison.OrdinalIgnoreCase))
                {
                    accepted.Add(hwId);
                }
            }
        }

        var earned = homeworkMeta.Where(x => accepted.Contains(x.HomeworkId)).Sum(x => x.Weight);
        var percent = (int)Math.Round((double)earned / total * 100.0, MidpointRounding.AwayFromZero);
        if (percent < 0) percent = 0;
        if (percent > 100) percent = 100;
        return percent;
    }

    private static DateTime? ToNullableDateTime(object? value)
    {
        if (value is null || value is DBNull)
        {
            return null;
        }

        if (value is DateTime dt)
        {
            return dt;
        }

        if (DateTime.TryParse(value.ToString(), CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal, out var parsed))
        {
            return parsed;
        }

        return null;
    }

    public async Task<IReadOnlyList<CategoryItem>> GetCategoriesAsync(CancellationToken cancellationToken = default)
    {
        var result = new List<CategoryItem>();

        await using var connection = new OleDbConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT ID_categorise, name_categori, discription FROM CATEGORIES ORDER BY ID_categorise";

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (reader is null)
        {
            return result;
        }

        while (await reader.ReadAsync(cancellationToken))
        {
            result.Add(new CategoryItem
            {
                ID_categorise = reader["ID_categorise"] is DBNull ? 0 : Convert.ToInt32(reader["ID_categorise"]),
                name_categori = reader["name_categori"]?.ToString() ?? string.Empty,
                discription = reader["discription"]?.ToString() ?? string.Empty
            });
        }

        return result;
    }

    public async Task<IReadOnlyList<CourseItem>> GetCoursesAsync(CancellationToken cancellationToken = default)
    {
        var result = new List<CourseItem>();

        await using var connection = new OleDbConnection(_connectionString);
        // OleDb + Access часто кидает TaskCanceledException при отмене HTTP запроса.
        await connection.OpenAsync(CancellationToken.None);
        var courseTable = GetExistingTableName(connection, "COURSES", "COURS");

        await using var command = connection.CreateCommand();
        command.CommandText = $"SELECT * FROM [{courseTable}] ORDER BY Course_name";

        await using var reader = await command.ExecuteReaderAsync(CancellationToken.None);
        if (reader is null)
        {
            return result;
        }

        while (await reader.ReadAsync(CancellationToken.None))
        {
            result.Add(MapCourse(reader));
        }

        var aggregates = await GetCourseRatingAggregatesAsync(CancellationToken.None);
        return ApplyReviewAggregatesToCourses(result, aggregates);
    }

    public async Task<CourseItem?> GetCourseByIdAsync(int courseId, CancellationToken cancellationToken = default)
    {
        if (courseId <= 0)
        {
            return null;
        }

        await using var connection = new OleDbConnection(_connectionString);
        await connection.OpenAsync(CancellationToken.None);
        var courseTable = GetExistingTableName(connection, "COURSES", "COURS");
        var idColumn = GetExistingColumnName(connection, courseTable, "ID_curs", "ID_cours");

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $"""
            SELECT TOP 1 *
            FROM [{courseTable}]
            WHERE [{idColumn}] = ?
            """;
        cmd.Parameters.AddWithValue("@p1", courseId);

        await using var reader = await cmd.ExecuteReaderAsync(CancellationToken.None);
        if (reader is null || !await reader.ReadAsync(CancellationToken.None))
        {
            return null;
        }

        var course = MapCourse(reader);
        // Рейтинг/кол-во отзывов подтянем безопасно (без отмены запроса).
        try
        {
            var aggr = await GetCourseRatingAggregatesAsync(CancellationToken.None);
            if (aggr.TryGetValue(course.ID_curs, out var s) && s.ReviewCount > 0)
            {
                return CourseItemWithRating(course, s.AverageRating, s.ReviewCount);
            }
        }
        catch
        {
            // ignore
        }

        return course;
    }

    public async Task<IReadOnlyList<CourseLessonItem>> GetLessonsByCourseIdAsync(int courseId, CancellationToken cancellationToken = default)
    {
        await using var connection = new OleDbConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        // В Access часто есть и LESSONS, и LESSON; раньше бралась первая по списку — иногда пустая копия.
        foreach (var preferred in new[] { "LESSONS", "LESSON" })
        {
            if (TryGetExistingTableName(connection, preferred) is not { } tableName)
            {
                continue;
            }

            try
            {
                var list = await ReadLessonsForCourseFromTableAsync(connection, tableName, courseId, cancellationToken);
                if (list.Count > 0)
                {
                    return list;
                }
            }
            catch (OleDbException)
            {
                // схема другой таблицы не подошла — пробуем следующую
            }
        }

        return new List<CourseLessonItem>();
    }

    public async Task<CourseLessonItem?> GetLessonByIdAsync(int lessonId, CancellationToken cancellationToken = default)
    {
        await using var connection = new OleDbConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        foreach (var preferred in new[] { "LESSONS", "LESSON" })
        {
            if (TryGetExistingTableName(connection, preferred) is not { } tableName)
            {
                continue;
            }

            try
            {
                var row = await ReadLessonByIdFromTableAsync(connection, tableName, lessonId, cancellationToken);
                if (row is not null)
                {
                    return row;
                }
            }
            catch (OleDbException)
            {
                // пробуем альтернативное имя таблицы
            }
        }

        return null;
    }

    public async Task<int> CreateLessonAdminAsync(
        int courseId,
        int? numberLesson,
        string lessonName,
        string videoUrl,
        string materialsUrl,
        string lessonDescription,
        CancellationToken cancellationToken = default)
    {
        await using var connection = new OleDbConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var lessonsTable = GetExistingTableName(connection, "LESSONS", "LESSON");
        var lessonIdCol = GetExistingColumnName(connection, lessonsTable, "ID_lesson", "ID_lessons");
        var courseCol = GetExistingColumnName(connection, lessonsTable, "ID_cours", "ID_curs", "ID_course");
        var nameCol = TryGetExistingColumnName(connection, lessonsTable, "Cours_name", "Course_name", "Lesson_name", "name");
        var numCol = TryGetExistingColumnName(connection, lessonsTable, "number_lesson", "Number_lesson", "lesson_number", "number", "num_lesson");
        var videoCol = TryGetExistingColumnName(connection, lessonsTable, "video_url", "videoUrl");
        var matCol = TryGetExistingColumnName(connection, lessonsTable, "meterials_url", "materials_url");
        var descCol = TryGetExistingColumnName(connection, lessonsTable, "course_discription", "course_description", "Cours_discription");

        var newId = await GetNextIdAsync(connection, lessonsTable, lessonIdCol, cancellationToken);

        var columns = new List<string> { $"[{lessonIdCol}]", $"[{courseCol}]" };
        var values = new List<object?> { newId, courseId };

        void Add(string? col, object? val)
        {
            if (col is null) return;
            columns.Add($"[{col}]");
            values.Add(val);
        }

        Add(nameCol, ToStringSafe(lessonName));
        Add(numCol, numberLesson.HasValue ? numberLesson.Value : (object)DBNull.Value);
        Add(videoCol, ToStringSafe(videoUrl));
        Add(matCol, ToStringSafe(materialsUrl));
        Add(descCol, string.IsNullOrWhiteSpace(lessonDescription) ? (object)DBNull.Value : ToStringSafe(lessonDescription));

        await using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = $"INSERT INTO [{lessonsTable}] ({string.Join(", ", columns)}) VALUES ({string.Join(", ", Enumerable.Repeat("?", columns.Count))})";
            for (var i = 0; i < values.Count; i++)
            {
                cmd.Parameters.AddWithValue($"@p{i + 1}", values[i] ?? DBNull.Value);
            }
            await cmd.ExecuteNonQueryAsync(cancellationToken);
        }

        // если у курса не указан первый урок — проставим
        if (courseId > 0)
        {
            try
            {
                var course = await GetCourseByIdAsync(courseId, cancellationToken);
                if (course is not null && course.ID_lesson <= 0)
                {
                    await UpdateCourseAdminAsync(
                        courseId,
                        newId,
                        course.ID_categorise,
                        course.price,
                        course.Course_name,
                        course.rating,
                        course.preview_url,
                        null,
                        cancellationToken);
                }
            }
            catch
            {
                // не критично
            }
        }

        return newId;
    }

    public async Task<bool> UpdateLessonAsync(
        int lessonId,
        int courseId,
        int? numberLesson,
        string lessonName,
        string videoUrl,
        string materialsUrl,
        string? lessonDescription,
        CancellationToken cancellationToken = default)
    {
        if (lessonId <= 0)
        {
            return false;
        }

        await using var connection = new OleDbConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        foreach (var preferred in new[] { "LESSONS", "LESSON" })
        {
            if (TryGetExistingTableName(connection, preferred) is not { } lessonsTable)
            {
                continue;
            }

            try
            {
                var lessonIdCol = TryGetExistingColumnName(connection, lessonsTable, "ID_lesson", "ID_lessons");
                if (lessonIdCol is null)
                {
                    continue;
                }

                var courseCol = TryGetExistingColumnName(connection, lessonsTable, "ID_cours", "ID_curs", "ID_course");
                var nameCol = TryGetExistingColumnName(connection, lessonsTable, "Cours_name", "Course_name", "Lesson_name", "name");
                var numCol = TryGetExistingColumnName(connection, lessonsTable, "number_lesson", "Number_lesson", "lesson_number", "number", "num_lesson");
                var videoCol = TryGetExistingColumnName(connection, lessonsTable, "video_url", "videoUrl");
                var matCol = TryGetExistingColumnName(connection, lessonsTable, "meterials_url", "materials_url");
                var descCol = TryGetExistingColumnName(connection, lessonsTable, "course_discription", "course_description", "Cours_discription");

                var sets = new List<string>();
                var values = new List<object?>();
                void Add(string? col, object? val)
                {
                    if (col is null) return;
                    sets.Add($"[{col}] = ?");
                    values.Add(val);
                }

                if (courseId > 0 && courseCol is not null)
                {
                    Add(courseCol, courseId);
                }
                Add(nameCol, ToStringSafe(lessonName));
                if (numCol is not null)
                {
                    Add(numCol, numberLesson.HasValue ? numberLesson.Value : (object)DBNull.Value);
                }
                Add(videoCol, NormalizeVideoForDb(videoUrl ?? string.Empty));
                Add(matCol, NormalizeMaterialsField(materialsUrl ?? string.Empty));
                if (descCol is not null)
                {
                    Add(descCol, string.IsNullOrWhiteSpace(lessonDescription) ? (object)DBNull.Value : ToStringSafe(lessonDescription));
                }

                if (sets.Count == 0)
                {
                    return false;
                }

                await using var cmd = connection.CreateCommand();
                cmd.CommandText = $"UPDATE [{lessonsTable}] SET {string.Join(", ", sets)} WHERE [{lessonIdCol}] = ?";
                for (var i = 0; i < values.Count; i++)
                {
                    cmd.Parameters.AddWithValue($"@p{i + 1}", values[i] ?? DBNull.Value);
                }
                cmd.Parameters.AddWithValue($"@p{values.Count + 1}", lessonId);
                var affected = await cmd.ExecuteNonQueryAsync(cancellationToken);
                if (affected is -1 or > 0)
                {
                    return true;
                }
            }
            catch (OleDbException)
            {
                // пробуем альтернативную таблицу
            }
        }

        return false;
    }

    public async Task<bool> DeleteLessonAsync(int lessonId, CancellationToken cancellationToken = default)
    {
        if (lessonId <= 0)
        {
            return false;
        }

        await using var connection = new OleDbConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        foreach (var preferred in new[] { "LESSONS", "LESSON" })
        {
            if (TryGetExistingTableName(connection, preferred) is not { } lessonsTable)
            {
                continue;
            }

            try
            {
                var lessonIdCol = TryGetExistingColumnName(connection, lessonsTable, "ID_lesson", "ID_lessons");
                var lessonCourseCol = TryGetExistingColumnName(connection, lessonsTable, "ID_cours", "ID_curs", "ID_course");
                var lessonNumberCol = TryGetExistingColumnName(connection, lessonsTable, "number_lesson", "lesson_number", "number", "Number_lesson");
                if (lessonIdCol is null || lessonCourseCol is null)
                {
                    continue;
                }

                var courseId = 0;
                await using (var lookup = connection.CreateCommand())
                {
                    lookup.CommandText = $"SELECT TOP 1 [{lessonCourseCol}] FROM [{lessonsTable}] WHERE [{lessonIdCol}] = ?";
                    lookup.Parameters.AddWithValue("@p1", lessonId);
                    courseId = ToInt(await lookup.ExecuteScalarAsync(cancellationToken));
                }

                if (courseId <= 0)
                {
                    continue;
                }

                await using (var deleteCmd = connection.CreateCommand())
                {
                    deleteCmd.CommandText = $"DELETE FROM [{lessonsTable}] WHERE [{lessonIdCol}] = ?";
                    deleteCmd.Parameters.AddWithValue("@p1", lessonId);
                    var affected = await deleteCmd.ExecuteNonQueryAsync(cancellationToken);
                    if (affected <= 0 && affected != -1)
                    {
                        continue;
                    }
                }

                // Если у курса был удалён "первый урок", подставим следующий по номеру/ID.
                try
                {
                    var courseTable = GetExistingTableName(connection, "COURSES", "COURSE");
                    var courseIdCol = GetExistingColumnName(connection, courseTable, "ID_curs", "ID_course");
                    var courseLessonCol = TryGetExistingColumnName(connection, courseTable, "ID_lesson", "ID_lessons");
                    if (courseLessonCol is not null)
                    {
                        var currentFirstLessonId = 0;
                        await using (var firstCmd = connection.CreateCommand())
                        {
                            firstCmd.CommandText = $"SELECT TOP 1 [{courseLessonCol}] FROM [{courseTable}] WHERE [{courseIdCol}] = ?";
                            firstCmd.Parameters.AddWithValue("@p1", courseId);
                            currentFirstLessonId = ToInt(await firstCmd.ExecuteScalarAsync(cancellationToken));
                        }

                        if (currentFirstLessonId == lessonId)
                        {
                            var replacementLessonId = 0;
                            var orderCol = lessonNumberCol ?? lessonIdCol;
                            await using (var replCmd = connection.CreateCommand())
                            {
                                replCmd.CommandText = $"""
                                    SELECT TOP 1 [{lessonIdCol}]
                                    FROM [{lessonsTable}]
                                    WHERE [{lessonCourseCol}] = ?
                                    ORDER BY [{orderCol}], [{lessonIdCol}]
                                    """;
                                replCmd.Parameters.AddWithValue("@p1", courseId);
                                replacementLessonId = ToInt(await replCmd.ExecuteScalarAsync(cancellationToken));
                            }

                            await using var upd = connection.CreateCommand();
                            upd.CommandText = $"UPDATE [{courseTable}] SET [{courseLessonCol}] = ? WHERE [{courseIdCol}] = ?";
                            upd.Parameters.AddWithValue("@p1", replacementLessonId > 0 ? replacementLessonId : (object)DBNull.Value);
                            upd.Parameters.AddWithValue("@p2", courseId);
                            _ = await upd.ExecuteNonQueryAsync(cancellationToken);
                        }
                    }
                }
                catch
                {
                    // Не блокируем удаление урока, если не удалось обновить курс.
                }

                return true;
            }
            catch (OleDbException)
            {
                // Пробуем альтернативную таблицу уроков
            }
        }

        return false;
    }

    private static async Task<List<CourseLessonItem>> ReadLessonsForCourseFromTableAsync(
        OleDbConnection connection,
        string lessonsTable,
        int courseId,
        CancellationToken cancellationToken)
    {
        var result = new List<CourseLessonItem>();
        var lessonCourseColumn = GetExistingColumnName(connection, lessonsTable, "ID_cours", "ID_curs", "ID_course");
        var lessonIdColumn = GetExistingColumnName(connection, lessonsTable, "ID_lesson", "ID_lessons");
        var lessonNumberColumn = TryGetExistingColumnName(connection, lessonsTable, "number_lesson", "lesson_number", "number", "Number_lesson");
        var orderByColumn = lessonNumberColumn ?? lessonIdColumn;
        var courseDescColumn = TryGetExistingColumnName(
            connection,
            lessonsTable,
            "course_discription",
            "course_description",
            "Cours_discription");

        await using var command = connection.CreateCommand();
        command.CommandText = $"""
            SELECT *
            FROM [{lessonsTable}]
            WHERE [{lessonCourseColumn}] = ?
            ORDER BY [{orderByColumn}], [{lessonIdColumn}]
            """;
        command.Parameters.AddWithValue("@p1", courseId);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (reader is null)
        {
            return result;
        }

        while (await reader.ReadAsync(cancellationToken))
        {
            result.Add(MapCourseLessonRow(reader, courseDescColumn));
        }

        return result;
    }

    private static async Task<CourseLessonItem?> ReadLessonByIdFromTableAsync(
        OleDbConnection connection,
        string lessonsTable,
        int lessonId,
        CancellationToken cancellationToken)
    {
        var lessonIdColumn = GetExistingColumnName(connection, lessonsTable, "ID_lesson", "ID_lessons");
        var courseDescColumn = TryGetExistingColumnName(
            connection,
            lessonsTable,
            "course_discription",
            "course_description",
            "Cours_discription");

        await using var command = connection.CreateCommand();
        command.CommandText = $"""
            SELECT *
            FROM [{lessonsTable}]
            WHERE [{lessonIdColumn}] = ?
            """;
        command.Parameters.AddWithValue("@p1", lessonId);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (reader is null || !await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return MapCourseLessonRow(reader, courseDescColumn);
    }

    private static CourseLessonItem MapCourseLessonRow(DbDataReader reader, string? courseDescColumn) =>
        new()
        {
            ID_cours = GetInt(reader, "ID_cours", "ID_curs"),
            ID_lesson = GetInt(reader, "ID_lesson", "ID_lessons"),
            number_lesson = ToNullableInt(GetValue(reader, "number_lesson", "Number_lesson", "lesson_number", "number", "num_lesson")),
            Cours_name = GetString(reader, "Cours_name", "Course_name", "Lesson_name", "name"),
            video_url = NormalizeUrlLikeField(GetString(reader, "video_url", "videoUrl")),
            meterials_url = NormalizeMaterialsField(GetString(reader, "meterials_url", "materials_url")),
            course_discription = NormalizeOptionalMemo(courseDescColumn is not null ? reader[courseDescColumn] : null)
        };

    private static string NormalizeMaterialsField(string raw)
    {
        // Для материалов часто прилетают пути на диске с тройными кавычками: """D:\file.docx"""
        // Здесь только аккуратно очищаем кавычки/пробелы, не трогаем сам путь.
        if (string.IsNullOrWhiteSpace(raw))
        {
            return string.Empty;
        }

        var s = raw.Trim();
        s = s.Trim().Trim('"').Trim();
        while (s.StartsWith("\"") && s.EndsWith("\"") && s.Length > 1)
        {
            s = s[1..^1].Trim();
        }

        return s.Trim();
    }

    private static string NormalizeUrlLikeField(string raw)
    {
        if (string.IsNullOrWhiteSpace(raw))
        {
            return string.Empty;
        }

        var s = raw.Trim().Trim('"').Trim('\t', '\r', '\n', ' ');
        while (s.Length > 0 && (s[0] == '"' || s[0] == '\''))
        {
            s = s.TrimStart('"', '\'').Trim();
        }

        return s.Trim();
    }

    /// <summary>
    /// Нормализует уроки в БД:
    /// - перенумеровывает number_lesson внутри каждого курса с 1
    /// - чистит video_url (если есть http(s) внутри строки — оставляет только ссылку)
    /// - чистит meterials_url (убирает лишние кавычки вокруг путей типа """D:\file.docx""")
    /// </summary>
    public async Task<int> NormalizeLessonsMediaAndOrderAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = new OleDbConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var lessonsTable = TryGetExistingTableName(connection, "LESSONS", "LESSON");
        if (string.IsNullOrWhiteSpace(lessonsTable))
        {
            return 0;
        }

        var lessonCourseColumn = GetExistingColumnName(connection, lessonsTable, "ID_cours", "ID_curs", "ID_course");
        var lessonIdColumn = GetExistingColumnName(connection, lessonsTable, "ID_lesson", "ID_lessons");
        var lessonNumberColumn = TryGetExistingColumnName(connection, lessonsTable, "number_lesson", "lesson_number", "number", "Number_lesson");
        var videoColumn = TryGetExistingColumnName(connection, lessonsTable, "video_url", "videoUrl");
        var materialsColumn = TryGetExistingColumnName(connection, lessonsTable, "meterials_url", "materials_url");

        if (lessonNumberColumn is null && videoColumn is null && materialsColumn is null)
        {
            return 0;
        }

        var rows = new List<(int CourseId, int LessonId, int? Number, string Video, string Materials)>();

        await using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = $"""
                SELECT [{lessonCourseColumn}] AS c, [{lessonIdColumn}] AS l
                {(lessonNumberColumn is null ? "" : $", [{lessonNumberColumn}] AS n")}
                {(videoColumn is null ? "" : $", [{videoColumn}] AS v")}
                {(materialsColumn is null ? "" : $", [{materialsColumn}] AS m")}
                FROM [{lessonsTable}]
                """;

            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
            if (reader is null)
            {
                return 0;
            }

            while (await reader.ReadAsync(cancellationToken))
            {
                var courseId = ToInt(reader["c"]);
                var lessonId = ToInt(reader["l"]);
                if (courseId <= 0 || lessonId <= 0)
                {
                    continue;
                }

                int? number = null;
                if (lessonNumberColumn is not null && reader["n"] is not DBNull)
                {
                    number = ToInt(reader["n"]);
                }

                var video = videoColumn is null ? "" : ToStringSafe(reader["v"]);
                var materials = materialsColumn is null ? "" : ToStringSafe(reader["m"]);
                rows.Add((courseId, lessonId, number, video, materials));
            }
        }

        var affected = 0;
        foreach (var group in rows.GroupBy(x => x.CourseId))
        {
            var ordered = group.OrderBy(x => x.LessonId).ToList();
            for (var i = 0; i < ordered.Count; i++)
            {
                var r = ordered[i];
                var desiredNumber = i + 1;

                var newVideo = videoColumn is null ? null : NormalizeVideoForDb(r.Video);
                var newMaterials = materialsColumn is null ? null : NormalizeMaterialsField(r.Materials);

                var needNumberUpdate = lessonNumberColumn is not null && (r.Number is null || r.Number.Value != desiredNumber);
                var needVideoUpdate = videoColumn is not null && !string.Equals((r.Video ?? "").Trim(), (newVideo ?? "").Trim(), StringComparison.Ordinal);
                var needMaterialsUpdate = materialsColumn is not null && !string.Equals((r.Materials ?? "").Trim(), (newMaterials ?? "").Trim(), StringComparison.Ordinal);

                if (!needNumberUpdate && !needVideoUpdate && !needMaterialsUpdate)
                {
                    continue;
                }

                var sets = new List<string>();
                if (needNumberUpdate && lessonNumberColumn is not null)
                {
                    sets.Add($"[{lessonNumberColumn}] = ?");
                }
                if (needVideoUpdate && videoColumn is not null)
                {
                    sets.Add($"[{videoColumn}] = ?");
                }
                if (needMaterialsUpdate && materialsColumn is not null)
                {
                    sets.Add($"[{materialsColumn}] = ?");
                }

                if (sets.Count == 0)
                {
                    continue;
                }

                await using var u = connection.CreateCommand();
                u.CommandText = $"UPDATE [{lessonsTable}] SET {string.Join(", ", sets)} WHERE [{lessonIdColumn}] = ?";

                if (needNumberUpdate && lessonNumberColumn is not null)
                {
                    u.Parameters.AddWithValue("@p_num", desiredNumber);
                }
                if (needVideoUpdate && videoColumn is not null)
                {
                    u.Parameters.AddWithValue("@p_vid", (object?)newVideo ?? DBNull.Value);
                }
                if (needMaterialsUpdate && materialsColumn is not null)
                {
                    u.Parameters.AddWithValue("@p_mat", (object?)newMaterials ?? DBNull.Value);
                }
                u.Parameters.AddWithValue("@p_id", r.LessonId);

                affected += await u.ExecuteNonQueryAsync(cancellationToken);
            }
        }

        return affected;
    }

    private static string NormalizeVideoForDb(string raw)
    {
        if (string.IsNullOrWhiteSpace(raw))
        {
            return string.Empty;
        }

        var s = raw.Trim().Replace('\\', '/');
        var httpsIdx = s.IndexOf("https://", StringComparison.OrdinalIgnoreCase);
        var httpIdx = s.IndexOf("http://", StringComparison.OrdinalIgnoreCase);
        var idx = httpsIdx >= 0 ? httpsIdx : httpIdx;
        if (idx >= 0)
        {
            return s[idx..].Trim();
        }

        return NormalizeUrlLikeField(raw);
    }

    /// <summary>
    /// Находит урок по ID домашней работы (таблица HOMEWORK/PROGRES).
    /// </summary>
    public async Task<int?> GetLessonIdByHomeworkIdAsync(int homeworkId, CancellationToken cancellationToken = default)
    {
        if (homeworkId <= 0)
        {
            return null;
        }

        await using var connection = new OleDbConnection(_connectionString);
        await connection.OpenAsync(CancellationToken.None);
        var progressTable = GetExistingTableName(connection, "HOMEWORK", "PROGRES");

        await using var command = connection.CreateCommand();
        command.CommandText = $"""
            SELECT TOP 1 ID_lesson
            FROM [{progressTable}]
            WHERE ID_homework = ?
            ORDER BY ID_lesson
            """;
        command.Parameters.AddWithValue("@p1", homeworkId);
        var obj = await command.ExecuteScalarAsync(CancellationToken.None);
        var lessonId = ToInt(obj);
        return lessonId > 0 ? lessonId : null;
    }

    public async Task<LessonProgressItem?> GetProgressByLessonIdAsync(int lessonId, CancellationToken cancellationToken = default)
    {
        await using var connection = new OleDbConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        var progressTable = GetExistingTableName(connection, "HOMEWORK", "PROGRES");

        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT ID_lesson, ID_homework, Progres_bal, status, grade
            FROM {0}
            WHERE ID_lesson = ?
            ORDER BY ID_homework
            """;
        command.CommandText = string.Format(command.CommandText, $"[{progressTable}]");
        command.Parameters.AddWithValue("@p1", lessonId);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (reader is null || !await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return new LessonProgressItem
        {
            ID_lesson = ToInt(reader["ID_lesson"]),
            ID_homework = ToInt(reader["ID_homework"]),
            Progres_bal = ToInt(reader["Progres_bal"]),
            status = ToStringSafe(reader["status"]),
            grade = ToNullableInt(reader["grade"])
        };
    }

    public async Task<int> UpsertHomeworkForLessonAsync(
        int lessonId,
        int? requestedHomeworkId,
        CancellationToken cancellationToken = default)
    {
        if (lessonId <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(lessonId));
        }

        await using var connection = new OleDbConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var progressTable = GetExistingTableName(connection, "HOMEWORK", "PROGRES");
        var lessonCol = GetExistingColumnName(connection, progressTable, "ID_lesson");
        var hwCol = GetExistingColumnName(connection, progressTable, "ID_homework");
        var progressBalCol = TryGetExistingColumnName(connection, progressTable, "Progres_bal", "progress_bal");
        var statusCol = TryGetExistingColumnName(connection, progressTable, "status", "state");
        var gradeCol = TryGetExistingColumnName(connection, progressTable, "grade", "mark");
        string NumExpr(string col) => $"CLng(IIf(IsNumeric([{col}]), [{col}], 0))";
        var lessonExpr = NumExpr(lessonCol);

        async Task<int> GetNextHomeworkIdAsync()
        {
            var maxHw = 0;
            await using var maxCmd = connection.CreateCommand();
            maxCmd.CommandText = $"SELECT [{hwCol}] FROM [{progressTable}]";
            await using var reader = await maxCmd.ExecuteReaderAsync(cancellationToken);
            if (reader is not null)
            {
                while (await reader.ReadAsync(cancellationToken))
                {
                    var current = ToInt(reader[0]);
                    if (current > maxHw) maxHw = current;
                }
            }
            return maxHw + 1;
        }

        var existingHomeworkId = 0;
        await using (var existingCmd = connection.CreateCommand())
        {
            existingCmd.CommandText = $"""
                SELECT TOP 1 [{hwCol}]
                FROM [{progressTable}]
                WHERE {lessonExpr} = ?
                ORDER BY {NumExpr(hwCol)} DESC
                """;
            existingCmd.Parameters.Add(new OleDbParameter("@p1", OleDbType.Integer) { Value = lessonId });
            existingHomeworkId = ToInt(await existingCmd.ExecuteScalarAsync(cancellationToken));
        }

        if (existingHomeworkId > 0 && (!requestedHomeworkId.HasValue || requestedHomeworkId.Value <= 0))
        {
            return existingHomeworkId;
        }

        var targetHomeworkId = requestedHomeworkId.GetValueOrDefault();
        if (targetHomeworkId <= 0)
        {
            targetHomeworkId = await GetNextHomeworkIdAsync();
        }

        if (existingHomeworkId > 0)
        {
            for (var attempt = 0; attempt < 20; attempt++)
            {
                try
                {
                    await using var update = connection.CreateCommand();
                    update.CommandText = $"UPDATE [{progressTable}] SET [{hwCol}] = ? WHERE {lessonExpr} = ?";
                    update.Parameters.Add(new OleDbParameter("@p1", OleDbType.Integer) { Value = targetHomeworkId });
                    update.Parameters.Add(new OleDbParameter("@p2", OleDbType.Integer) { Value = lessonId });
                    _ = await update.ExecuteNonQueryAsync(cancellationToken);
                    return targetHomeworkId;
                }
                catch (OleDbException) when (!requestedHomeworkId.HasValue || requestedHomeworkId.Value <= 0)
                {
                    targetHomeworkId = await GetNextHomeworkIdAsync();
                }
            }
        }

        var columns = new List<string> { $"[{lessonCol}]", $"[{hwCol}]" };
        var values = new List<object?> { lessonId, targetHomeworkId };
        if (progressBalCol is not null) { columns.Add($"[{progressBalCol}]"); values.Add(0); }
        if (statusCol is not null) { columns.Add($"[{statusCol}]"); values.Add("open"); }
        if (gradeCol is not null) { columns.Add($"[{gradeCol}]"); values.Add(DBNull.Value); }

        for (var attempt = 0; attempt < 20; attempt++)
        {
            try
            {
                await using var insert = connection.CreateCommand();
                insert.CommandText = $"INSERT INTO [{progressTable}] ({string.Join(", ", columns)}) VALUES ({string.Join(", ", Enumerable.Repeat("?", values.Count))})";
                insert.Parameters.AddWithValue("@p1", lessonId);
                insert.Parameters.AddWithValue("@p2", targetHomeworkId);
                for (var i = 2; i < values.Count; i++)
                {
                    insert.Parameters.AddWithValue($"@p{i + 1}", values[i] ?? DBNull.Value);
                }
                _ = await insert.ExecuteNonQueryAsync(cancellationToken);
                return targetHomeworkId;
            }
            catch (OleDbException) when (!requestedHomeworkId.HasValue || requestedHomeworkId.Value <= 0)
            {
                targetHomeworkId = await GetNextHomeworkIdAsync();
                values[1] = targetHomeworkId;
            }
        }

        throw new InvalidOperationException("Не удалось создать/обновить домашнее задание для урока.");
    }

    public async Task<UserItem?> GetUserByIdAsync(int userId, CancellationToken cancellationToken = default)
    {
        await using var connection = new OleDbConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var blockedCol = TryGetExistingColumnName(connection, "USER", "is_blocked");
        var blockedUntilCol = TryGetExistingColumnName(connection, "USER", "blocked_until");
        var blockReasonCol = TryGetExistingColumnName(connection, "USER", "block_reason");
        var deletedCol = TryGetExistingColumnName(connection, "USER", "is_deleted");
        var rubCol = TryGetExistingColumnName(connection, "USER", "balance_rub", "refund_balance_rub", "rub_balance");

        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT *
            FROM [USER]
            WHERE ID_user = ?
            """;
        command.Parameters.AddWithValue("@p1", userId);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (reader is null || !await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return MapUserRow(reader, blockedCol, blockedUntilCol, blockReasonCol, deletedCol, rubCol);
    }

    public async Task<UserItem?> GetUserByCredentialsAsync(string email, string password, CancellationToken cancellationToken = default)
    {
        var normalizedEmail = NormalizeEmail(email);
        var quotedEmail = $"\"{normalizedEmail}\"";

        await using var connection = new OleDbConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var blockedCol = TryGetExistingColumnName(connection, "USER", "is_blocked");
        var blockedUntilCol = TryGetExistingColumnName(connection, "USER", "blocked_until");
        var blockReasonCol = TryGetExistingColumnName(connection, "USER", "block_reason");
        var deletedCol = TryGetExistingColumnName(connection, "USER", "is_deleted");
        var rubCol = TryGetExistingColumnName(connection, "USER", "balance_rub", "refund_balance_rub", "rub_balance");

        await using var command = connection.CreateCommand();
        command.CommandText = $"""
            SELECT *
            FROM [USER]
            WHERE (email = ? OR email = ?) AND [password] = ?
            """;
        command.Parameters.AddWithValue("@p1", normalizedEmail);
        command.Parameters.AddWithValue("@p2", quotedEmail);
        command.Parameters.AddWithValue("@p3", password);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (reader is null || !await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return MapUserRow(reader, blockedCol, blockedUntilCol, blockReasonCol, deletedCol, rubCol);
    }

    public async Task<UserItem?> GetUserByEmailAsync(string email, CancellationToken cancellationToken = default)
    {
        var normalizedEmail = NormalizeEmail(email);
        var quotedEmail = $"\"{normalizedEmail}\"";

        await using var connection = new OleDbConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT *
            FROM [USER]
            WHERE email = ? OR email = ?
            """;
        command.Parameters.AddWithValue("@p1", normalizedEmail);
        command.Parameters.AddWithValue("@p2", quotedEmail);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (reader is null || !await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        var blockedCol = TryGetExistingColumnName(connection, "USER", "is_blocked");
        var blockedUntilCol = TryGetExistingColumnName(connection, "USER", "blocked_until");
        var blockReasonCol = TryGetExistingColumnName(connection, "USER", "block_reason");
        var deletedCol = TryGetExistingColumnName(connection, "USER", "is_deleted");
        var rubCol = TryGetExistingColumnName(connection, "USER", "balance_rub", "refund_balance_rub", "rub_balance");
        return MapUserRow(reader, blockedCol, blockedUntilCol, blockReasonCol, deletedCol, rubCol);
    }

    private static UserItem MapUserRow(DbDataReader reader, string? blockedCol, string? blockedUntilCol, string? blockReasonCol, string? deletedCol, string? rubCol) =>
        new()
        {
            ID_user = ToInt(reader["ID_user"]),
            ID_curs = ToNullableInt(GetValue(reader, "ID_curs")),
            ID__homework = ToNullableInt(GetValue(reader, "ID__homework")),
            reward_coins = ToInt(GetValue(reader, "reward_coins")),
            email = ToStringSafe(GetValue(reader, "email")),
            phone = ToStringSafe(GetValue(reader, "phone")),
            role = ToStringSafe(GetValue(reader, "role")),
            full_name = ToStringSafe(GetValue(reader, "full_name")),
            balanse_coins = ToNullableInt(GetValue(reader, "balanse_coins")),
            balance_rub = rubCol is null ? null : ToNullableDecimal(reader[rubCol]),
            is_blocked = blockedCol is null ? null : ToNullableBool(reader[blockedCol]),
            blocked_until = blockedUntilCol is null ? null : ToNullableDateTime(reader[blockedUntilCol]),
            block_reason = blockReasonCol is null ? null : ToStringSafe(reader[blockReasonCol]),
            is_deleted = deletedCol is null ? null : ToNullableBool(reader[deletedCol])
        };

    private static bool? ToNullableBool(object? value)
    {
        if (value is null || value is DBNull) return null;
        return Convert.ToBoolean(value);
    }

    public async Task<IReadOnlyList<UserItem>> GetAllUsersAsync(CancellationToken cancellationToken = default)
    {
        var result = new List<UserItem>();
        await using var connection = new OleDbConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var blockedCol = TryGetExistingColumnName(connection, "USER", "is_blocked");
        var blockedUntilCol = TryGetExistingColumnName(connection, "USER", "blocked_until");
        var blockReasonCol = TryGetExistingColumnName(connection, "USER", "block_reason");
        var deletedCol = TryGetExistingColumnName(connection, "USER", "is_deleted");
        var rubCol = TryGetExistingColumnName(connection, "USER", "balance_rub", "refund_balance_rub", "rub_balance");

        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT * FROM [USER] ORDER BY ID_user DESC";

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (reader is null)
        {
            return result;
        }

        while (await reader.ReadAsync(cancellationToken))
        {
            result.Add(MapUserRow(reader, blockedCol, blockedUntilCol, blockReasonCol, deletedCol, rubCol));
        }

        return result;
    }

    public async Task<bool> EmailExistsAsync(string email, CancellationToken cancellationToken = default)
    {
        var normalizedEmail = NormalizeEmail(email);
        var quotedEmail = $"\"{normalizedEmail}\"";

        await using var connection = new OleDbConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT COUNT(*) FROM [USER] WHERE email = ? OR email = ?";
        command.Parameters.AddWithValue("@p1", normalizedEmail);
        command.Parameters.AddWithValue("@p2", quotedEmail);

        var value = await command.ExecuteScalarAsync(cancellationToken);
        return ToInt(value) > 0;
    }

    public async Task<int> CreateUserAsync(
        string firstName,
        string lastName,
        string email,
        string phone,
        string password,
        CancellationToken cancellationToken = default)
    {
        await using var connection = new OleDbConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        var normalizedEmail = NormalizeEmail(email);
        var createdUserId = 0;
        var registrationCompleted = false;

        for (var attempt = 0; attempt < 8; attempt++)
        {
            using var tx = connection.BeginTransaction();
            try
            {
                var quotedEmail = $"\"{normalizedEmail}\"";
                await using (var emailCheck = connection.CreateCommand())
                {
                    emailCheck.Transaction = tx;
                    emailCheck.CommandText = """
                        SELECT COUNT(*)
                        FROM [USER]
                        WHERE [email] = ? OR [email] = ?
                        """;
                    emailCheck.Parameters.AddWithValue("@p1", normalizedEmail);
                    emailCheck.Parameters.AddWithValue("@p2", quotedEmail);
                    if (ToInt(await emailCheck.ExecuteScalarAsync(cancellationToken)) > 0)
                    {
                        throw new InvalidOperationException("Пользователь с таким email уже существует.");
                    }
                }

                var normalizedPhone = phone.Trim();
                await using (var phoneCheck = connection.CreateCommand())
                {
                    phoneCheck.Transaction = tx;
                    phoneCheck.CommandText = """
                        SELECT COUNT(*)
                        FROM [USER]
                        WHERE [phone] = ?
                        """;
                    phoneCheck.Parameters.AddWithValue("@p1", normalizedPhone);
                    if (ToInt(await phoneCheck.ExecuteScalarAsync(cancellationToken)) > 0)
                    {
                        throw new InvalidOperationException("Пользователь с таким телефоном уже зарегистрирован.");
                    }
                }

                createdUserId = await GetNextUserIdUsingAccessMaxAsync(connection, tx, cancellationToken);
                var registrationRewardCoins = await AllocateRegistrationRewardCoinsAsync(
                    connection,
                    tx,
                    createdUserId,
                    cancellationToken);
        var fullName = $"{firstName} {lastName}".Trim();

        await using var command = connection.CreateCommand();
                command.Transaction = tx;
                BuildUserRegistrationInsertCommand(
                    connection,
                    command,
                    createdUserId,
                    registrationRewardCoins,
                    normalizedEmail,
                    normalizedPhone,
                    fullName,
                    password);

        await command.ExecuteNonQueryAsync(cancellationToken);
                tx.Commit();
                registrationCompleted = true;
                break;
            }
            catch (OleDbException ex) when (attempt < 7 && IsLikelyDuplicateKeyOleDb(ex))
            {
                try
                {
                    tx.Rollback();
                }
                catch
                {
                    // ignore rollback errors after duplicate-key failure
                }

                await Task.Delay(40 * (attempt + 1), cancellationToken);
            }
            catch
            {
                try
                {
                    tx.Rollback();
                }
                catch
                {
                    // ignore
                }

                throw;
            }
        }

        if (!registrationCompleted)
        {
            throw new InvalidOperationException(
                "Регистрация не завершена: не удалось сохранить пользователя после нескольких попыток.");
        }

        try
        {
            await using var postConnection = new OleDbConnection(_connectionString);
            await postConnection.OpenAsync(cancellationToken);
            if (createdUserId > 0)
            {
                await TryLinkRegistrationReviewAfterUserInsertAsync(postConnection, null, createdUserId, cancellationToken);
                await EnsureShowAdRecordForUserAsync(postConnection, createdUserId, cancellationToken);
            }
        }
        catch (OleDbException)
        {
            // Do not fail registration if auxiliary ad tables have strict or inconsistent relations.
        }

        return createdUserId;
    }

    private static void BuildUserRegistrationInsertCommand(
        OleDbConnection connection,
        OleDbCommand command,
        int createdUserId,
        int registrationRewardCoins,
        string normalizedEmail,
        string normalizedPhone,
        string fullName,
        string password)
    {
        var homeworkColumn = TryGetExistingColumnName(connection, "USER", "ID__homework", "ID_homework");
        var columns = new List<string> { "[ID_user]", "[ID_curs]" };
        if (homeworkColumn is not null)
        {
            columns.Add($"[{homeworkColumn}]");
        }

        columns.AddRange(new[]
        {
            "[reward_coins]", "[email]", "[phone]", "[role]", "[full_name]", "[balanse_coins]", "[password]"
        });

        var placeholders = string.Join(", ", Enumerable.Repeat("?", columns.Count));
        command.CommandText = $"INSERT INTO [USER] ({string.Join(", ", columns)}) VALUES ({placeholders})";

        var values = new List<object?>
        {
            createdUserId,
            DBNull.Value
        };
        if (homeworkColumn is not null)
        {
            values.Add(DBNull.Value);
        }

        values.AddRange(new object?[]
        {
            registrationRewardCoins,
            normalizedEmail,
            normalizedPhone,
            "student",
            fullName,
            0,
            password
        });

        for (var i = 0; i < values.Count; i++)
        {
            command.Parameters.AddWithValue($"@r{i}", values[i] ?? DBNull.Value);
        }
    }

    private static bool IsLikelyDuplicateKeyOleDb(OleDbException ex)
    {
        var message = ex.Message ?? string.Empty;
        return message.Contains("повторяющихся", StringComparison.OrdinalIgnoreCase)
            || message.Contains("duplicate", StringComparison.OrdinalIgnoreCase);
    }

    private static async Task<HashSet<int>> ReadDistinctPositiveRewardCoinsFromUserAsync(
        OleDbConnection connection,
        OleDbTransaction? transaction,
        CancellationToken cancellationToken)
    {
        var set = new HashSet<int>();
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "SELECT [reward_coins] FROM [USER]";
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (reader is null)
        {
            return set;
        }

        while (await reader.ReadAsync(cancellationToken))
        {
            var v = ToInt(reader["reward_coins"]);
            if (v > 0)
            {
                set.Add(v);
            }
        }

        return set;
    }

    /// <summary>
    /// reward_coins при регистрации — 0 (игровые/рекламные идентификаторы; бонусные баллы — <c>balanse_coins</c>).
    /// </summary>
    private static Task<int> AllocateRegistrationRewardCoinsAsync(
        OleDbConnection connection,
        OleDbTransaction? transaction,
        int plannedUserId,
        CancellationToken cancellationToken) =>
        Task.FromResult(0);

    private static async Task SeedAdAndShowAdRewardTierIfMissingAsync(
        OleDbConnection connection,
        OleDbTransaction? transaction,
        int plannedUserId,
        int rewardCoins,
        CancellationToken cancellationToken)
    {
        await using var existsCommand = connection.CreateCommand();
        existsCommand.Transaction = transaction;
        existsCommand.CommandText = "SELECT COUNT(*) FROM [SHOW_AD] WHERE [reward_coins] = ?";
        existsCommand.Parameters.AddWithValue("@p1", rewardCoins);
        var existing = ToInt(await existsCommand.ExecuteScalarAsync(cancellationToken));
        if (existing > 0)
        {
            return;
        }

        var maxAdId = 0;
        await using (var maxCommand = connection.CreateCommand())
        {
            maxCommand.Transaction = transaction;
            maxCommand.CommandText = "SELECT MAX([ID_AD]) FROM [AD]";
            maxAdId = ToInt(await maxCommand.ExecuteScalarAsync(cancellationToken));
        }

        var adId = Math.Max(300, maxAdId) + 1;

        await using (var insertAd = connection.CreateCommand())
        {
            insertAd.Transaction = transaction;
            insertAd.CommandText = """
                INSERT INTO [AD] ([ID_AD], [AD_type], [reward_coins])
                VALUES (?, ?, ?)
                """;
            insertAd.Parameters.AddWithValue("@p1", adId);
            insertAd.Parameters.AddWithValue("@p2", "auto");
            insertAd.Parameters.AddWithValue("@p3", rewardCoins);
            await insertAd.ExecuteNonQueryAsync(cancellationToken);
        }

        await using (var insertShow = connection.CreateCommand())
        {
            insertShow.Transaction = transaction;
            insertShow.CommandText = """
                INSERT INTO [SHOW_AD] ([ID_AD], [ID_user], [reward_coins])
                VALUES (?, ?, ?)
                """;
            insertShow.Parameters.AddWithValue("@p1", adId);
            insertShow.Parameters.AddWithValue("@p2", plannedUserId);
            insertShow.Parameters.AddWithValue("@p3", rewardCoins);
            await insertShow.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    private static async Task<List<int>> ReadPositiveRewardCoinsFromTableAsync(
        OleDbConnection connection,
        OleDbTransaction? transaction,
        string tableSql,
        CancellationToken cancellationToken)
    {
        var result = new List<int>();
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = $"SELECT reward_coins FROM {tableSql}";
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (reader is null)
        {
            return result;
        }

        while (await reader.ReadAsync(cancellationToken))
        {
            var v = ToInt(reader["reward_coins"]);
            if (v > 0)
            {
                result.Add(v);
            }
        }

        return result;
    }

    private static async Task<int> GetNextUserIdUsingAccessMaxAsync(
        OleDbConnection connection,
        OleDbTransaction? transaction,
        CancellationToken cancellationToken)
    {
        var usedIds = new HashSet<int>();
        await MergeParsedUserIdsFromQueryAsync(
            connection,
            transaction,
            usedIds,
            "SELECT [ID_user] FROM [USER]",
            "ID_user",
            suppressOleDbExceptions: false,
            cancellationToken);

        await TryMergeUserIdsFromTableColumnAsync(
            connection, transaction, usedIds, "SHOW_AD", "ID_user", cancellationToken);
        await TryMergeUserIdsFromTableColumnAsync(
            connection, transaction, usedIds, "ENROLLMENTS", "ID_user", cancellationToken);
        await TryMergeUserIdsFromTableColumnAsync(
            connection, transaction, usedIds, "PAY", "ID_user", cancellationToken);

        var reviewsTable = TryGetExistingTableName(connection, "REVIEWS", "REVIEW");
        if (reviewsTable is not null)
        {
            var reviewUserCol = TryGetExistingColumnName(connection, reviewsTable, "ID_user");
            if (reviewUserCol is not null)
            {
                await MergeParsedUserIdsFromQueryAsync(
                    connection,
                    transaction,
                    usedIds,
                    $"SELECT [{reviewUserCol}] FROM [{reviewsTable}]",
                    reviewUserCol,
                    suppressOleDbExceptions: true,
                    cancellationToken);
            }
        }

        var candidate = Math.Max(1000, usedIds.Count == 0 ? 1000 : usedIds.Max()) + 1;
        while (usedIds.Contains(candidate))
        {
            candidate++;
        }

        return candidate;
    }

    private static async Task TryMergeUserIdsFromTableColumnAsync(
        OleDbConnection connection,
        OleDbTransaction? transaction,
        HashSet<int> usedIds,
        string preferredTableName,
        string preferredColumnName,
        CancellationToken cancellationToken)
    {
        var table = TryGetExistingTableName(connection, preferredTableName);
        if (table is null)
        {
            return;
        }

        var column = TryGetExistingColumnName(connection, table, preferredColumnName);
        if (column is null)
        {
            return;
        }

        await MergeParsedUserIdsFromQueryAsync(
            connection,
            transaction,
            usedIds,
            $"SELECT [{column}] FROM [{table}]",
            column,
            suppressOleDbExceptions: true,
            cancellationToken);
    }

    private static async Task MergeParsedUserIdsFromQueryAsync(
        OleDbConnection connection,
        OleDbTransaction? transaction,
        HashSet<int> usedIds,
        string selectSql,
        string readerKey,
        bool suppressOleDbExceptions,
        CancellationToken cancellationToken)
    {
        try
        {
            await using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = selectSql;
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (reader is null)
            {
                return;
            }

            while (await reader.ReadAsync(cancellationToken))
            {
                if (TryParseUserId(reader[readerKey], out var id) && id > 0)
                {
                    usedIds.Add(id);
                }
            }
        }
        catch (OleDbException) when (suppressOleDbExceptions)
        {
            // схема локально отличается — пропускаем источник
        }
    }

    private static bool TryParseUserId(object? value, out int id)
    {
        id = 0;
        if (value is null || value is DBNull)
        {
            return false;
        }

        if (value is int i)
        {
            id = i;
            return id > 0;
        }

        if (value is long l)
        {
            if (l is < 1 or > int.MaxValue)
            {
                return false;
            }

            id = (int)l;
            return true;
        }

        var text = value.ToString()?.Trim()?.Trim('"');
        if (string.IsNullOrEmpty(text))
        {
            return false;
        }

        return int.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out id) && id > 0;
    }

    /// <summary>
    /// Отдельный шаг после INSERT USER: создаёт строку в REVIEWS и проставляет USER.ID_review (связь схемы Access).
    /// </summary>
    private static async Task TryLinkRegistrationReviewAfterUserInsertAsync(
        OleDbConnection connection,
        OleDbTransaction? transaction,
        int userId,
        CancellationToken cancellationToken)
    {
        var reviewsTable = TryGetExistingTableName(connection, "REVIEWS", "REVIEW");
        if (string.IsNullOrWhiteSpace(reviewsTable))
        {
            return;
        }

        string reviewIdColumn;
        string reviewCourseColumn;
        string reviewUserColumn;
        string reviewRatingColumn;
        string reviewCommentColumn;
        string reviewCreatedAtColumn;
        try
        {
            reviewIdColumn = GetExistingColumnName(connection, reviewsTable, "ID_review");
            reviewCourseColumn = GetExistingColumnName(connection, reviewsTable, "ID_cours", "ID_curs");
            reviewUserColumn = GetExistingColumnName(connection, reviewsTable, "ID_user");
            reviewRatingColumn = GetExistingColumnName(connection, reviewsTable, "rating");
            reviewCommentColumn = GetExistingColumnName(connection, reviewsTable, "comment_text", "comment");
            reviewCreatedAtColumn = GetExistingColumnName(connection, reviewsTable, "created_at", "create_at");
        }
        catch (InvalidOperationException)
        {
            return;
        }

        var newReviewId = await GetNextRegistrationReviewIdAsync(
            connection,
            transaction,
            reviewsTable,
            reviewIdColumn,
            cancellationToken);
        var courseId = await GetFirstCourseIdForRegistrationAsync(connection, transaction, cancellationToken);
        if (courseId <= 0)
        {
            courseId = 101;
        }

        try
        {
            await using var insert = connection.CreateCommand();
            insert.Transaction = transaction;
            insert.CommandText = $"""
                INSERT INTO [{reviewsTable}] ([{reviewIdColumn}], [{reviewCourseColumn}], [{reviewUserColumn}], [{reviewRatingColumn}], [{reviewCommentColumn}], [{reviewCreatedAtColumn}])
                VALUES (?, ?, ?, ?, ?, ?)
                """;
            insert.Parameters.AddWithValue("@p1", newReviewId);
            insert.Parameters.AddWithValue("@p2", courseId);
            insert.Parameters.AddWithValue("@p3", userId);
            insert.Parameters.AddWithValue("@p4", 1m);
            insert.Parameters.AddWithValue("@p5", " ");
            insert.Parameters.AddWithValue("@p6", DateTime.Now);
            await insert.ExecuteNonQueryAsync(cancellationToken);
        }
        catch (OleDbException)
        {
            return;
        }

        try
        {
            await using var update = connection.CreateCommand();
            update.Transaction = transaction;
            update.CommandText = """
                UPDATE [USER]
                SET [ID_review] = ?
                WHERE [ID_user] = ?
                """;
            update.Parameters.AddWithValue("@p1", newReviewId);
            update.Parameters.AddWithValue("@p2", userId);
            await update.ExecuteNonQueryAsync(cancellationToken);
        }
        catch (OleDbException)
        {
            // USER уже создан; отзыв остаётся в REVIEWS без обратной ссылки
        }
    }

    private static async Task<int> GetNextRegistrationReviewIdAsync(
        OleDbConnection connection,
        OleDbTransaction? transaction,
        string reviewsTable,
        string reviewIdColumn,
        CancellationToken cancellationToken)
    {
        var max = 0;
        await using (var cmd = connection.CreateCommand())
        {
            cmd.Transaction = transaction;
            cmd.CommandText = $"SELECT [{reviewIdColumn}] FROM [{reviewsTable}]";
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
            while (reader is not null && await reader.ReadAsync(cancellationToken))
            {
                max = Math.Max(max, ToInt(reader[reviewIdColumn]));
            }
        }

        await using (var cmd = connection.CreateCommand())
        {
            cmd.Transaction = transaction;
            cmd.CommandText = "SELECT [ID_review] FROM [USER]";
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
            while (reader is not null && await reader.ReadAsync(cancellationToken))
            {
                var raw = reader["ID_review"];
                if (raw is null || raw is DBNull)
                {
                    continue;
                }

                max = Math.Max(max, ToInt(raw));
            }
        }

        return max + 1;
    }

    private static async Task<int> GetFirstCourseIdForRegistrationAsync(
        OleDbConnection connection,
        OleDbTransaction? transaction,
        CancellationToken cancellationToken)
    {
        try
        {
            var courseTable = GetExistingTableName(connection, "COURSES", "COURS");
            var idColumn = GetExistingColumnName(connection, courseTable, "ID_curs", "ID_cours");
            await using var cmd = connection.CreateCommand();
            cmd.Transaction = transaction;
            cmd.CommandText = $"""
                SELECT TOP 1 [{idColumn}]
                FROM [{courseTable}]
                ORDER BY [{idColumn}]
                """;
            var scalar = await cmd.ExecuteScalarAsync(cancellationToken);
            return ToInt(scalar);
        }
        catch (OleDbException)
        {
            return 0;
        }
        catch (InvalidOperationException)
        {
            return 0;
        }
    }

    private static async Task EnsureShowAdRecordForUserAsync(OleDbConnection connection, int userId, CancellationToken cancellationToken)
    {
        var schema = connection.GetSchema("Tables");
        var hasShowAd = false;
        var hasAd = false;

        foreach (DataRow row in schema.Rows)
        {
            var tableType = row["TABLE_TYPE"]?.ToString();
            var tableName = row["TABLE_NAME"]?.ToString();
            if (!string.Equals(tableType, "TABLE", StringComparison.OrdinalIgnoreCase) || string.IsNullOrWhiteSpace(tableName))
            {
                continue;
            }

            if (string.Equals(tableName, "SHOW_AD", StringComparison.OrdinalIgnoreCase))
            {
                hasShowAd = true;
            }
            else if (string.Equals(tableName, "AD", StringComparison.OrdinalIgnoreCase))
            {
                hasAd = true;
            }
        }

        if (!hasShowAd || !hasAd)
        {
            return;
        }

        await using var checkCommand = connection.CreateCommand();
        checkCommand.CommandText = "SELECT COUNT(*) FROM [SHOW_AD] WHERE ID_user = ?";
        checkCommand.Parameters.AddWithValue("@p1", userId);
        var existing = await checkCommand.ExecuteScalarAsync(cancellationToken);
        if (ToInt(existing) > 0)
        {
            return;
        }

        var adId = 0;
        var reward = 0;
        await using (var adCommand = connection.CreateCommand())
        {
            adCommand.CommandText = """
                SELECT TOP 1 A.ID_AD, A.reward_coins
                FROM [AD] AS A
                LEFT JOIN [SHOW_AD] AS S ON A.ID_AD = S.ID_AD
                WHERE S.ID_AD IS NULL
                ORDER BY A.ID_AD
                """;
            await using var adReader = await adCommand.ExecuteReaderAsync(cancellationToken);
            if (adReader is not null && await adReader.ReadAsync(cancellationToken))
            {
                adId = ToInt(adReader["ID_AD"]);
                reward = ToInt(adReader["reward_coins"]);
            }
        }

        if (adId <= 0)
        {
            var maxAdId = 0;
            await using (var maxCommand = connection.CreateCommand())
            {
                maxCommand.CommandText = "SELECT MAX(ID_AD) FROM [AD]";
                var maxValue = await maxCommand.ExecuteScalarAsync(cancellationToken);
                maxAdId = ToInt(maxValue);
            }

            adId = Math.Max(300, maxAdId) + 1;
            reward = 0;

            await using var addAdCommand = connection.CreateCommand();
            addAdCommand.CommandText = """
                INSERT INTO [AD] (ID_AD, AD_type, reward_coins)
                VALUES (?, ?, ?)
                """;
            addAdCommand.Parameters.AddWithValue("@p1", adId);
            addAdCommand.Parameters.AddWithValue("@p2", "auto");
            addAdCommand.Parameters.AddWithValue("@p3", reward);
            await addAdCommand.ExecuteNonQueryAsync(cancellationToken);
        }

        await using var insertCommand = connection.CreateCommand();
        insertCommand.CommandText = """
            INSERT INTO [SHOW_AD] (ID_AD, ID_user, reward_coins)
            VALUES (?, ?, ?)
            """;
        insertCommand.Parameters.AddWithValue("@p1", adId);
        insertCommand.Parameters.AddWithValue("@p2", userId);
        insertCommand.Parameters.AddWithValue("@p3", reward);
        await insertCommand.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<AdminStatsItem> GetAdminStatsAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = new OleDbConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        var courseTable = GetExistingTableName(connection, "COURSES", "COURS");
        var enrollmentTable = GetExistingTableName(connection, "ENROLLMENTS");
        var payTable = GetExistingTableName(connection, "PAY");
        var showAdTable = GetExistingTableName(connection, "SHOW_AD");

        var totalUsers = await ExecuteScalarIntAsync(connection, "SELECT COUNT(*) FROM [USER]", cancellationToken);
        var studentsCount = await ExecuteScalarIntAsync(connection, "SELECT COUNT(*) FROM [USER] WHERE role = 'student'", cancellationToken);
        var activeCoursesCount = await ExecuteScalarIntAsync(connection, $"SELECT COUNT(*) FROM [{courseTable}]", cancellationToken);
        var avgPrice = await ExecuteScalarDecimalAsync(connection, $"SELECT AVG(price) FROM [{courseTable}]", cancellationToken);
        var potentialRevenue = await ExecuteScalarDecimalAsync(connection, $"SELECT SUM(price) FROM [{courseTable}]", cancellationToken);
        var totalEnrollments = await ExecuteScalarIntAsync(connection, $"SELECT COUNT(*) FROM [{enrollmentTable}]", cancellationToken);
        var totalPayments = await ExecuteScalarIntAsync(connection, $"SELECT COUNT(*) FROM [{payTable}]", cancellationToken);
        var totalPaidRevenue = await GetTotalPaidRevenueRobustAsync(cancellationToken);
        var totalAdViews = await ExecuteScalarIntAsync(connection, $"SELECT COUNT(*) FROM [{showAdTable}]", cancellationToken);

        return new AdminStatsItem
        {
            TotalUsers = totalUsers,
            StudentsCount = studentsCount,
            ActiveCoursesCount = activeCoursesCount,
            AvgCoursePrice = avgPrice,
            TotalPotentialRevenue = potentialRevenue,
            TotalPaidRevenue = totalPaidRevenue,
            TotalEnrollments = totalEnrollments,
            TotalPayments = totalPayments,
            TotalAdViews = totalAdViews
        };
    }

    public async Task<AdminDashboardVm> GetAdminDashboardAsync(CancellationToken cancellationToken = default)
    {
        var stats = await GetAdminStatsAsync(cancellationToken);

        // Графики: если в БД нет дат по USER/PAY, строим максимально честно из того, что есть.
        // 1) Выручка по месяцам — по датам создания курсов (create_at) как "потенциальная выручка".
        // 2) Новые пользователи по месяцам — если в USER нет даты, распределяем по ID_user (псевдо‑период).
        var months = LastSixMonthsLabels();

        var revenue = await GetPotentialRevenueByCourseCreateMonthAsync(cancellationToken);
        var revenueSeries = months.Select(m => revenue.TryGetValue(m, out var v) ? v : 0m).ToList();

        var newUsers = await GetNewUsersByMonthIfPossibleAsync(cancellationToken);
        var newUsersSeries = months.Select(m => newUsers.TryGetValue(m, out var v) ? v : 0).ToList();

        return new AdminDashboardVm
        {
            Stats = stats,
            Months = months,
            RevenueByMonth = revenueSeries,
            NewUsersByMonth = newUsersSeries
        };
    }

    private static IReadOnlyList<string> LastSixMonthsLabels()
    {
        var list = new List<string>(6);
        var now = DateTime.Now;
        for (var i = 5; i >= 0; i--)
        {
            var dt = new DateTime(now.Year, now.Month, 1).AddMonths(-i);
            list.Add(dt.ToString("MMM", CultureInfo.GetCultureInfo("ru-RU")));
        }
        return list;
    }

    private async Task<Dictionary<string, decimal>> GetPotentialRevenueByCourseCreateMonthAsync(CancellationToken cancellationToken)
    {
        var map = new Dictionary<string, decimal>(StringComparer.OrdinalIgnoreCase);
        await using var connection = new OleDbConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        var courseTable = GetExistingTableName(connection, "COURSES", "COURS");
        var createCol = TryGetExistingColumnName(connection, courseTable, "create_at", "created_at", "create_date");
        var priceCol = TryGetExistingColumnName(connection, courseTable, "price", "Price");
        if (createCol is null || priceCol is null)
        {
            return map;
        }

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $"SELECT [{createCol}] AS c, [{priceCol}] AS p FROM [{courseTable}]";
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        if (reader is null)
        {
            return map;
        }

        var ru = CultureInfo.GetCultureInfo("ru-RU");
        while (await reader.ReadAsync(cancellationToken))
        {
            if (reader["c"] is DBNull)
            {
                continue;
            }
            var dt = Convert.ToDateTime(reader["c"]);
            var key = new DateTime(dt.Year, dt.Month, 1).ToString("MMM", ru);
            var price = ToDecimal(reader["p"]);
            map[key] = map.TryGetValue(key, out var cur) ? cur + price : price;
        }

        return map;
    }

    private async Task<Dictionary<string, int>> GetNewUsersByMonthIfPossibleAsync(CancellationToken cancellationToken)
    {
        var map = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        await using var connection = new OleDbConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        // Пытаемся найти дату создания пользователя (не всегда есть).
        var createdCol = TryGetExistingColumnName(connection, "USER", "created_at", "create_at", "create_date", "reg_date", "registered_at");
        if (createdCol is not null)
        {
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = $"SELECT [{createdCol}] AS c FROM [USER]";
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
            if (reader is null)
            {
                return map;
            }
            var ru = CultureInfo.GetCultureInfo("ru-RU");
            while (await reader.ReadAsync(cancellationToken))
            {
                if (reader["c"] is DBNull) continue;
                var dt = Convert.ToDateTime(reader["c"]);
                var key = new DateTime(dt.Year, dt.Month, 1).ToString("MMM", ru);
                map[key] = map.TryGetValue(key, out var cur) ? cur + 1 : 1;
            }
            return map;
        }

        // Фолбэк: распределяем по последним 6 месяцам равномерно, но количество берём из БД.
        var total = await ExecuteScalarIntAsync(connection, "SELECT COUNT(*) FROM [USER]", cancellationToken);
        var months = LastSixMonthsLabels();
        if (months.Count == 0)
        {
            return map;
        }
        var baseVal = total / months.Count;
        var rem = total % months.Count;
        for (var i = 0; i < months.Count; i++)
        {
            map[months[i]] = baseVal + (i < rem ? 1 : 0);
        }
        return map;
    }

    public async Task<IReadOnlyList<EnrollmentItem>> GetEnrollmentsByUserIdAsync(int userId, CancellationToken cancellationToken = default)
    {
        var result = new List<EnrollmentItem>();

        // OleDb/Access часто "ломается" при отмене HTTP-токеном и может ронять запрос.
        // Для чтения личных данных лучше не отменять запрос к Access.
        var ct = CancellationToken.None;

        await using var connection = new OleDbConnection(_connectionString);
        await connection.OpenAsync(ct);
        var enrollmentTable = GetExistingTableName(connection, "ENROLLMENTS");
        var enrollmentIdColumn = GetExistingColumnName(connection, enrollmentTable, "ID_enrolment");
        var enrollmentCourseColumn = GetEnrollmentCourseColumn(connection, enrollmentTable);
        var enrollmentUserColumn = GetExistingColumnName(connection, enrollmentTable, "ID_user");
        var enrollmentHomeworkColumn = TryGetExistingColumnName(connection, enrollmentTable, "ID_homework");
        var enrollmentPayMethodColumn = TryGetExistingColumnName(connection, enrollmentTable, "PAY_method");
        var enrollmentStatusColumn = TryGetEnrollmentStatusColumn(connection, enrollmentTable);
        var enrollmentPriceColumn = TryGetExistingColumnName(connection, enrollmentTable, "price");

        var payMethodSelect = enrollmentPayMethodColumn is null ? "'card'" : $"[{enrollmentPayMethodColumn}]";
        var statusSelect = enrollmentStatusColumn is null ? "'completed'" : $"[{enrollmentStatusColumn}]";
        var priceSelect = enrollmentPriceColumn is null ? "0" : $"[{enrollmentPriceColumn}]";
        var homeworkSelect = enrollmentHomeworkColumn is null ? "0" : $"[{enrollmentHomeworkColumn}]";

        await using var command = connection.CreateCommand();
        command.CommandText = $"""
            SELECT
                [{enrollmentIdColumn}] AS enrollment_id,
                [{enrollmentCourseColumn}] AS enrollment_course_id,
                [{enrollmentUserColumn}] AS enrollment_user_id,
                {homeworkSelect} AS enrollment_homework_id,
                {payMethodSelect} AS enrollment_pay_method,
                {statusSelect} AS enrollment_status,
                {priceSelect} AS enrollment_price
            FROM [{enrollmentTable}]
            WHERE [{enrollmentUserColumn}] = ?
            ORDER BY [{enrollmentIdColumn}] DESC
            """;
        command.Parameters.AddWithValue("@p1", userId);

        await using var reader = await command.ExecuteReaderAsync(ct);
        if (reader is null)
        {
            return result;
        }

        while (await reader.ReadAsync(ct))
        {
            result.Add(new EnrollmentItem
            {
                ID_enrolment = ToInt(reader["enrollment_id"]),
                ID_cours = ToInt(reader["enrollment_course_id"]),
                ID_user = ToInt(reader["enrollment_user_id"]),
                PAY_method = ToStringSafe(reader["enrollment_pay_method"]),
                status = ToStringSafe(reader["enrollment_status"]),
                price = ToDecimal(reader["enrollment_price"])
            });
        }

        return result;
    }

    public async Task<EnrollmentItem?> GetEnrollmentByUserAndCourseAsync(int userId, int courseId, CancellationToken cancellationToken = default)
    {
        var enrollments = await GetEnrollmentsByUserIdAsync(userId, cancellationToken);
        return enrollments
            .Where(e => e.ID_cours == courseId)
            .OrderByDescending(e => e.ID_enrolment)
            .FirstOrDefault();
    }

    public async Task<bool> IsUserEnrolledInCourseAsync(int userId, int courseId, CancellationToken cancellationToken = default)
    {
        if (userId <= 0 || courseId <= 0)
        {
            return false;
        }

        var enrollment = await GetEnrollmentByUserAndCourseAsync(userId, courseId, cancellationToken);
        if (enrollment is null)
        {
            return false;
        }

        var st = (enrollment.status ?? string.Empty).Trim();
        if (string.Equals(st, "cancelled", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }
        if (string.Equals(st, "refund_requested", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        return true;
    }

    /// <summary>
    /// Можно ли отправлять ДЗ по курсу (статус записи не «обучение завершено» и не отмена).
    /// </summary>
    public async Task<bool> CanUserSubmitHomeworkAsync(int userId, int courseId, CancellationToken cancellationToken = default)
    {
        if (userId <= 0 || courseId <= 0)
        {
            return false;
        }

        var enrollment = await GetEnrollmentByUserAndCourseAsync(userId, courseId, cancellationToken);
        if (enrollment is null)
        {
            return false;
        }

        var st = (enrollment.status ?? string.Empty).Trim();
        if (string.Equals(st, "cancelled", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        if (string.Equals(st, "finished", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        return true;
    }

    public async Task<bool> UnenrollUserFromCourseAsync(int userId, int courseId, CancellationToken cancellationToken = default)
    {
        await using var connection = new OleDbConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        var enrollmentTable = GetExistingTableName(connection, "ENROLLMENTS");
        var enrollmentCourseColumn = GetEnrollmentCourseColumn(connection, enrollmentTable);
        var enrollmentUserColumn = GetExistingColumnName(connection, enrollmentTable, "ID_user");
        var enrollmentStatusColumn = TryGetEnrollmentStatusColumn(connection, enrollmentTable);

        await using var command = connection.CreateCommand();
        if (enrollmentStatusColumn is not null)
        {
            command.CommandText = $"""
                UPDATE [{enrollmentTable}]
                SET [{enrollmentStatusColumn}] = ?
                WHERE [{enrollmentUserColumn}] = ? AND [{enrollmentCourseColumn}] = ?
                """;
            command.Parameters.AddWithValue("@p1", "cancelled");
            command.Parameters.AddWithValue("@p2", userId);
            command.Parameters.AddWithValue("@p3", courseId);
            return await command.ExecuteNonQueryAsync(cancellationToken) > 0;
        }

        // если в таблице нет status — удаляем запись
        command.CommandText = $"""
            DELETE FROM [{enrollmentTable}]
            WHERE [{enrollmentUserColumn}] = ? AND [{enrollmentCourseColumn}] = ?
            """;
        command.Parameters.AddWithValue("@p1", userId);
        command.Parameters.AddWithValue("@p2", courseId);
        return await command.ExecuteNonQueryAsync(cancellationToken) > 0;
    }

    /// <summary>
    /// В старых БД столбца status в ENROLLMENTS может не быть — добавляем TEXT-поле.
    /// </summary>
    public async Task EnsureEnrollmentStatusColumnAsync(CancellationToken cancellationToken = default)
    {
        if (_enrollmentStatusColumnEnsured)
        {
            return;
        }

        await _schemaLock.WaitAsync(CancellationToken.None);
        try
        {
            if (_enrollmentStatusColumnEnsured)
            {
                return;
            }

            await using var connection = new OleDbConnection(_connectionString);
            await connection.OpenAsync(CancellationToken.None);
            var enrollmentTable = GetExistingTableName(connection, "ENROLLMENTS");
            var statusCol = TryGetEnrollmentStatusColumn(connection, enrollmentTable);
            if (statusCol is null)
            {
                await using var alter = connection.CreateCommand();
                alter.CommandText = $"ALTER TABLE [{enrollmentTable}] ADD COLUMN [status] TEXT(255)";
                try
                {
                    await alter.ExecuteNonQueryAsync(CancellationToken.None);
                }
                catch (OleDbException)
                {
                    // столбец мог появиться параллельно или другое имя — ниже перепроверим
                }

                // Access: схема после ALTER может не обновиться на том же соединении — переподключаемся.
                await connection.CloseAsync();
                await connection.OpenAsync(CancellationToken.None);
            }

            _enrollmentStatusColumnEnsured = TryGetEnrollmentStatusColumn(connection, enrollmentTable) is not null;
        }
        finally
        {
            _schemaLock.Release();
        }
    }

    public async Task<bool> SetEnrollmentStatusAsync(
        int userId,
        int courseId,
        string status,
        CancellationToken cancellationToken = default)
    {
        if (userId <= 0 || courseId <= 0)
        {
            return false;
        }

        await EnsureEnrollmentStatusColumnAsync(cancellationToken);

        await using var connection = new OleDbConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        var enrollmentTable = GetExistingTableName(connection, "ENROLLMENTS");
        var enrollmentCourseColumn = GetEnrollmentCourseColumn(connection, enrollmentTable);
        var enrollmentUserColumn = GetExistingColumnName(connection, enrollmentTable, "ID_user", "user_id", "ID_users");
        var enrollmentIdColumn = GetExistingColumnName(connection, enrollmentTable, "ID_enrolment", "ID_enrollment", "enrollment_id");
        var statusColumns = GetEnrollmentStatusColumns(connection, enrollmentTable);
        if (statusColumns.Count == 0)
        {
            return false;
        }

        // Обновляем только последнюю запись (иначе при наличии 2-3 оплат получится 2-3 "заявки").
        var latestId = 0;
        await using (var findLatest = connection.CreateCommand())
        {
            findLatest.CommandText = $"""
                SELECT TOP 1 [{enrollmentIdColumn}]
                FROM [{enrollmentTable}]
                WHERE [{enrollmentUserColumn}] = ? AND [{enrollmentCourseColumn}] = ?
                ORDER BY [{enrollmentIdColumn}] DESC
                """;
            findLatest.Parameters.AddWithValue("@p1", userId);
            findLatest.Parameters.AddWithValue("@p2", courseId);
            latestId = ToInt(await findLatest.ExecuteScalarAsync(cancellationToken));
        }
        if (latestId <= 0)
        {
            return false;
        }

        // Не создаём/не "размножаем" заявку повторным кликом: если последняя запись уже refund_requested — ок.
        if (string.Equals(status, "refund_requested", StringComparison.OrdinalIgnoreCase))
        {
            await using var check = connection.CreateCommand();
            var statusSelectCols = string.Join(", ", statusColumns.Select(c => $"[{c}]"));
            check.CommandText = $"""
                SELECT TOP 1 {statusSelectCols}
                FROM [{enrollmentTable}]
                WHERE [{enrollmentIdColumn}] = ?
                """;
            check.Parameters.AddWithValue("@p1", latestId);
            await using var r = await check.ExecuteReaderAsync(cancellationToken);
            if (r is not null && await r.ReadAsync(cancellationToken))
            {
                foreach (var c in statusColumns)
                {
                    var v = ToStringSafe(r[c]).Trim();
                    if (string.Equals(v, "refund_requested", StringComparison.OrdinalIgnoreCase))
                    {
                        return true;
                    }
                    if (!string.IsNullOrWhiteSpace(v))
                    {
                        break;
                    }
                }
            }
        }

        await using (var command = connection.CreateCommand())
        {
            var sets = string.Join(", ", statusColumns.Select(c => $"[{c}] = ?"));
            command.CommandText = $"""
                UPDATE [{enrollmentTable}]
                SET {sets}
                WHERE [{enrollmentIdColumn}] = ? AND [{enrollmentUserColumn}] = ? AND [{enrollmentCourseColumn}] = ?
                """;
            var p = 1;
            foreach (var _ in statusColumns)
            {
                command.Parameters.AddWithValue($"@p{p++}", status);
            }
            command.Parameters.AddWithValue($"@p{p++}", latestId);
            command.Parameters.AddWithValue($"@p{p++}", userId);
            command.Parameters.AddWithValue($"@p{p}", courseId);
            _ = await command.ExecuteNonQueryAsync(cancellationToken);
        }

        // В старых/грязных данных может существовать несколько строк по одному курсу.
        // Чтобы в админке не появлялось 2-5 заявок, гарантируем: refund_requested только на последней строке.
        if (string.Equals(status, "refund_requested", StringComparison.OrdinalIgnoreCase))
        {
            var whereAnyRequested = string.Join(" OR ", statusColumns.Select(c => $"LCase(Trim([{c}]))='refund_requested'"));
            await using var clearOld = connection.CreateCommand();
            var sets = string.Join(", ", statusColumns.Select(c => $"[{c}] = ?"));
            clearOld.CommandText = $"""
                UPDATE [{enrollmentTable}]
                SET {sets}
                WHERE [{enrollmentUserColumn}] = ? AND [{enrollmentCourseColumn}] = ?
                  AND [{enrollmentIdColumn}] <> ?
                  AND ({whereAnyRequested})
                """;
            var p = 1;
            foreach (var _ in statusColumns)
            {
                clearOld.Parameters.AddWithValue($"@p{p++}", "cancelled");
            }
            clearOld.Parameters.AddWithValue($"@p{p++}", userId);
            clearOld.Parameters.AddWithValue($"@p{p++}", courseId);
            clearOld.Parameters.AddWithValue($"@p{p}", latestId);
            _ = await clearOld.ExecuteNonQueryAsync(cancellationToken);
        }

        await using var verify = connection.CreateCommand();
        var selectCols = string.Join(", ", statusColumns.Select(c => $"[{c}]"));
        verify.CommandText = $"""
            SELECT TOP 1 {selectCols}
            FROM [{enrollmentTable}]
            WHERE [{enrollmentIdColumn}] = ? AND [{enrollmentUserColumn}] = ? AND [{enrollmentCourseColumn}] = ?
            """;
        verify.Parameters.AddWithValue("@p1", latestId);
        verify.Parameters.AddWithValue("@p2", userId);
        verify.Parameters.AddWithValue("@p3", courseId);
        await using var vr = await verify.ExecuteReaderAsync(cancellationToken);
        if (vr is null || !await vr.ReadAsync(cancellationToken))
        {
            return false;
        }
        foreach (var c in statusColumns)
        {
            var v = ToStringSafe(vr[c]).Trim();
            if (!string.IsNullOrWhiteSpace(v))
            {
                return string.Equals(v, status, StringComparison.OrdinalIgnoreCase);
            }
        }
        return false;
    }

    public async Task<bool> UpdateCourseAdminAsync(
        int courseId,
        int firstLessonId,
        int categoryId,
        decimal price,
        string courseName,
        decimal ratingFallback,
        string previewUrl,
        int? teacherUserId,
        CancellationToken cancellationToken = default)
    {
        await using var connection = new OleDbConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        var courseTable = GetExistingTableName(connection, "COURSES", "COURS");
        var idColumn = GetExistingColumnName(connection, courseTable, "ID_curs", "ID_cours");

        // Колонки могут отличаться по регистру/неймингу в Access
        var lessonCol = TryGetExistingColumnName(connection, courseTable, "ID_lesson", "ID_lessons");
        var catCol = TryGetExistingColumnName(connection, courseTable, "ID_categorise", "ID_category");
        var priceCol = TryGetExistingColumnName(connection, courseTable, "price", "Price");
        var nameCol = TryGetExistingColumnName(connection, courseTable, "Course_name", "Cours_name", "course_name");
        var ratingCol = TryGetExistingColumnName(connection, courseTable, "rating", "Rating");
        var previewCol = TryGetExistingColumnName(connection, courseTable, "preview_url", "Preview_url", "preview");
        var teacherCol = TryGetExistingColumnName(connection, courseTable, "teacher_user_id");

        var sets = new List<string>();
        var values = new List<object?>();
        void Add(string? col, object? val)
        {
            if (col is null) return;
            sets.Add($"[{col}] = ?");
            values.Add(val);
        }

        Add(lessonCol, firstLessonId);
        Add(catCol, categoryId);
        Add(priceCol, price);
        Add(nameCol, ToStringSafe(courseName));
        Add(ratingCol, ratingFallback);
        Add(previewCol, ToStringSafe(previewUrl));
        Add(teacherCol, teacherUserId.HasValue ? teacherUserId.Value : (object)DBNull.Value);

        if (sets.Count == 0)
        {
            return false;
        }

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $"UPDATE [{courseTable}] SET {string.Join(", ", sets)} WHERE [{idColumn}] = ?";
        for (var i = 0; i < values.Count; i++)
        {
            cmd.Parameters.AddWithValue($"@p{i + 1}", values[i] ?? DBNull.Value);
        }
        cmd.Parameters.AddWithValue($"@p{values.Count + 1}", courseId);
        return await cmd.ExecuteNonQueryAsync(cancellationToken) > 0;
    }

    public async Task<int> CreateCourseAdminAsync(
        int firstLessonId,
        int categoryId,
        decimal price,
        string courseName,
        decimal ratingFallback,
        string previewUrl,
        int? teacherUserId,
        CancellationToken cancellationToken = default)
    {
        await using var connection = new OleDbConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        var courseTable = GetExistingTableName(connection, "COURSES", "COURS");
        var idColumn = GetExistingColumnName(connection, courseTable, "ID_curs", "ID_cours");

        var lessonCol = TryGetExistingColumnName(connection, courseTable, "ID_lesson", "ID_lessons");
        var catCol = TryGetExistingColumnName(connection, courseTable, "ID_categorise", "ID_category");
        var priceCol = TryGetExistingColumnName(connection, courseTable, "price", "Price");
        var nameCol = TryGetExistingColumnName(connection, courseTable, "Course_name", "Cours_name", "course_name");
        var ratingCol = TryGetExistingColumnName(connection, courseTable, "rating", "Rating");
        var createCol = TryGetExistingColumnName(connection, courseTable, "create_at", "created_at", "create_date");
        var previewCol = TryGetExistingColumnName(connection, courseTable, "preview_url", "Preview_url", "preview");
        var teacherCol = TryGetExistingColumnName(connection, courseTable, "teacher_user_id");

        var newId = await GetNextIdAsync(connection, courseTable, idColumn, cancellationToken);

        var columns = new List<string> { $"[{idColumn}]" };
        var values = new List<object?> { newId };

        void Add(string? col, object? val)
        {
            if (col is null) return;
            columns.Add($"[{col}]");
            values.Add(val);
        }

        Add(lessonCol, firstLessonId > 0 ? firstLessonId : (object)DBNull.Value);
        Add(catCol, categoryId);
        Add(priceCol, price);
        Add(nameCol, ToStringSafe(courseName));
        Add(ratingCol, ratingFallback);
        Add(createCol, DateTime.Now);
        Add(previewCol, ToStringSafe(previewUrl));
        Add(teacherCol, teacherUserId.HasValue ? teacherUserId.Value : (object)DBNull.Value);

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $"INSERT INTO [{courseTable}] ({string.Join(", ", columns)}) VALUES ({string.Join(", ", Enumerable.Repeat("?", columns.Count))})";
        for (var i = 0; i < values.Count; i++)
        {
            cmd.Parameters.AddWithValue($"@p{i + 1}", values[i] ?? DBNull.Value);
        }
        await cmd.ExecuteNonQueryAsync(cancellationToken);
        return newId;
    }

    public async Task EnsureCourseTeacherColumnAsync(CancellationToken cancellationToken = default)
    {
        if (_courseTeacherSchemaEnsured)
        {
            return;
        }

        // Миграции схемы не должны падать из-за отмены HTTP-запроса.
        await _schemaLock.WaitAsync(CancellationToken.None);
        try
        {
            if (_courseTeacherSchemaEnsured)
            {
                return;
            }

            await using var connection = new OleDbConnection(_connectionString);
            await connection.OpenAsync(CancellationToken.None);
            var courseTable = GetExistingTableName(connection, "COURSES", "COURS");

            // Если колонка уже существует — ничего не делаем (важно для повторных запусков).
            if (TryGetExistingColumnName(connection, courseTable, "teacher_user_id", "ID_teacher", "teacher_id", "teacher") is null)
            {
                await TryAddColumnAsync(connection, courseTable, "teacher_user_id", "INTEGER", CancellationToken.None);
            }
            _courseTeacherSchemaEnsured = true;
        }
        finally
        {
            _schemaLock.Release();
        }
    }

    private async Task EnsureTeacherCoursesTableAsync(CancellationToken cancellationToken = default)
    {
        if (_teacherCoursesSchemaEnsured)
        {
            return;
        }

        await _schemaLock.WaitAsync(CancellationToken.None);
        try
        {
            if (_teacherCoursesSchemaEnsured)
            {
                return;
            }

            await using var connection = new OleDbConnection(_connectionString);
            await connection.OpenAsync(CancellationToken.None);

            var existing = TryGetExistingTableName(connection, "TEACHER_COURSES", "TEACHER_COURSE", "COURSE_TEACHERS");
            if (existing is not null)
            {
                _teacherCoursesSchemaEnsured = true;
                return;
            }

            await using var cmd = connection.CreateCommand();
            cmd.CommandText = """
                CREATE TABLE TEACHER_COURSES (
                    teacher_user_id INTEGER,
                    course_id INTEGER
                )
                """;
            await cmd.ExecuteNonQueryAsync(CancellationToken.None);
            _teacherCoursesSchemaEnsured = true;
        }
        finally
        {
            _schemaLock.Release();
        }
    }

    public async Task<IReadOnlySet<int>> GetAssignedCourseIdsForTeacherAsync(int teacherUserId, CancellationToken cancellationToken = default)
    {
        var set = new HashSet<int>();
        if (teacherUserId <= 0)
        {
            return set;
        }

        await EnsureTeacherCoursesTableAsync(cancellationToken);
        await using var connection = new OleDbConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        var mapTable = GetExistingTableName(connection, "TEACHER_COURSES", "TEACHER_COURSE", "COURSE_TEACHERS");

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $"""
            SELECT course_id
            FROM [{mapTable}]
            WHERE teacher_user_id = ?
            """;
        cmd.Parameters.AddWithValue("@p1", teacherUserId);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        if (reader is null)
        {
            return set;
        }

        while (await reader.ReadAsync(cancellationToken))
        {
            var id = ToInt(reader["course_id"]);
            if (id > 0)
            {
                set.Add(id);
            }
        }

        return set;
    }

    public async Task<IReadOnlyDictionary<int, int>> GetTeacherCourseAssignmentsAsync(CancellationToken cancellationToken = default)
    {
        // courseId -> teacherUserId (если несколько — берём первую)
        var dict = new Dictionary<int, int>();

        await EnsureTeacherCoursesTableAsync(cancellationToken);
        await using var connection = new OleDbConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        var mapTable = GetExistingTableName(connection, "TEACHER_COURSES", "TEACHER_COURSE", "COURSE_TEACHERS");

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $"""
            SELECT teacher_user_id, course_id
            FROM [{mapTable}]
            """;

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        if (reader is null)
        {
            return dict;
        }

        while (await reader.ReadAsync(cancellationToken))
        {
            var t = ToInt(reader["teacher_user_id"]);
            var c = ToInt(reader["course_id"]);
            if (t > 0 && c > 0 && !dict.ContainsKey(c))
            {
                dict[c] = t;
            }
        }

        return dict;
    }

    public async Task<bool> AssignTeacherToCourseAsync(int teacherUserId, int courseId, CancellationToken cancellationToken = default)
    {
        if (teacherUserId <= 0 || courseId <= 0)
        {
            return false;
        }

        // Основной источник правды: TEACHER_COURSES (поддержка "много учителей на один курс").
        await EnsureTeacherCoursesTableAsync(cancellationToken);
        await using (var connection = new OleDbConnection(_connectionString))
        {
            await connection.OpenAsync(cancellationToken);
            var mapTable = GetExistingTableName(connection, "TEACHER_COURSES", "TEACHER_COURSE", "COURSE_TEACHERS");

            await using (var exists = connection.CreateCommand())
            {
                exists.CommandText = $"""
                    SELECT COUNT(*)
                    FROM [{mapTable}]
                    WHERE teacher_user_id = ? AND course_id = ?
                    """;
                exists.Parameters.AddWithValue("@p1", teacherUserId);
                exists.Parameters.AddWithValue("@p2", courseId);
                if (ToInt(await exists.ExecuteScalarAsync(cancellationToken)) > 0)
                {
                    return true;
                }
            }

            await using (var insert = connection.CreateCommand())
            {
                insert.CommandText = $"""
                    INSERT INTO [{mapTable}] (teacher_user_id, course_id)
                    VALUES (?, ?)
                    """;
                insert.Parameters.AddWithValue("@p1", teacherUserId);
                insert.Parameters.AddWithValue("@p2", courseId);
                var affected = await insert.ExecuteNonQueryAsync(cancellationToken);
                if (affected is not (-1 or > 0))
                {
                    return false;
                }
            }
        }

        // 2) Дополнительно пытаемся синхронизировать колонку в COURSES (если она есть) — но это не обязательное условие.
        try
        {
            _ = await SetCourseTeacherAsync(courseId, teacherUserId, cancellationToken);
        }
        catch
        {
            // игнор: может не быть колонки/прав
        }
        return true;
    }

    public async Task<bool> UnassignTeacherFromCourseAsync(int teacherUserId, int courseId, CancellationToken cancellationToken = default)
    {
        if (teacherUserId <= 0 || courseId <= 0)
        {
            return false;
        }

        var any = false;

        // 1) Снимаем в TEACHER_COURSES
        await EnsureTeacherCoursesTableAsync(cancellationToken);
        await using (var connection = new OleDbConnection(_connectionString))
        {
            await connection.OpenAsync(cancellationToken);
            var mapTable = GetExistingTableName(connection, "TEACHER_COURSES", "TEACHER_COURSE", "COURSE_TEACHERS");
            await using var del = connection.CreateCommand();
            del.CommandText = $"""
                DELETE FROM [{mapTable}]
                WHERE teacher_user_id = ? AND course_id = ?
                """;
            del.Parameters.AddWithValue("@p1", teacherUserId);
            del.Parameters.AddWithValue("@p2", courseId);
            var affected = await del.ExecuteNonQueryAsync(cancellationToken);
            if (affected is -1 or > 0)
            {
                any = true;
            }
        }

        // 2) Пытаемся снять в COURSES.teacher_user_id (если используется)
        try
        {
            var course = await GetCourseByIdAsync(courseId, cancellationToken);
            if (course?.teacher_user_id == teacherUserId)
            {
                var ok = await SetCourseTeacherAsync(courseId, null, cancellationToken);
                if (ok)
                {
                    any = true;
                }
            }
        }
        catch
        {
            // ignore
        }
        return any;
    }

    public async Task<bool> UnassignTeacherFromAllCoursesAsync(int teacherUserId, CancellationToken cancellationToken = default)
    {
        if (teacherUserId <= 0)
        {
            return false;
        }

        var any = false;

        // 1) Убираем из COURSES.teacher_user_id (если используется)
        try
        {
            await EnsureCourseTeacherColumnAsync(CancellationToken.None);
            await using var connection = new OleDbConnection(_connectionString);
            await connection.OpenAsync(cancellationToken);
            var courseTable = GetExistingTableName(connection, "COURSES", "COURS");
            var idColumn = GetExistingColumnName(connection, courseTable, "ID_curs", "ID_cours");
            var teacherCol = TryGetExistingColumnName(connection, courseTable, "teacher_user_id", "ID_teacher", "teacher_id", "teacher");
            if (teacherCol is not null)
            {
                await using var upd = connection.CreateCommand();
                upd.CommandText = $"""
                    UPDATE [{courseTable}]
                    SET [{teacherCol}] = NULL
                    WHERE [{teacherCol}] = ?
                    """;
                upd.Parameters.AddWithValue("@p1", teacherUserId);
                var affected = await upd.ExecuteNonQueryAsync(cancellationToken);
                if (affected is -1 or > 0)
                {
                    any = true;
                }
            }
        }
        catch
        {
            // ignore
        }

        // 2) Убираем из TEACHER_COURSES
        try
        {
            await EnsureTeacherCoursesTableAsync(cancellationToken);
            await using var connection = new OleDbConnection(_connectionString);
            await connection.OpenAsync(cancellationToken);
            var mapTable = GetExistingTableName(connection, "TEACHER_COURSES", "TEACHER_COURSE", "COURSE_TEACHERS");
            await using var del = connection.CreateCommand();
            del.CommandText = $"""
                DELETE FROM [{mapTable}]
                WHERE teacher_user_id = ?
                """;
            del.Parameters.AddWithValue("@p1", teacherUserId);
            var affected = await del.ExecuteNonQueryAsync(cancellationToken);
            if (affected is -1 or > 0)
            {
                any = true;
            }
        }
        catch
        {
            // ignore
        }

        return any;
    }

    public async Task<IReadOnlyList<CourseItem>> GetCoursesByTeacherIdAsync(int teacherUserId, CancellationToken cancellationToken = default)
    {
        await EnsureCourseTeacherColumnAsync(CancellationToken.None);

        var result = new List<CourseItem>();
        await using var connection = new OleDbConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        var courseTable = GetExistingTableName(connection, "COURSES", "COURS");
        var teacherCol = TryGetExistingColumnName(connection, courseTable, "teacher_user_id", "ID_teacher", "teacher_id", "teacher");
        if (teacherCol is not null)
        {
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = $"SELECT * FROM [{courseTable}] WHERE [{teacherCol}] = ? ORDER BY Course_name";
            cmd.Parameters.AddWithValue("@p1", teacherUserId);

            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
            if (reader is not null)
            {
                while (await reader.ReadAsync(cancellationToken))
                {
                    result.Add(MapCourse(reader));
                }
            }
        }

        // Всегда добавляем назначения из fallback-таблицы (на случай когда колонка есть, но не используется).
        var idsFromMap = await GetAssignedCourseIdsForTeacherAsync(teacherUserId, cancellationToken);
        if (idsFromMap.Count > 0)
        {
            var all = await GetCoursesAsync(cancellationToken);
            foreach (var c in all)
            {
                if (idsFromMap.Contains(c.ID_curs) && result.All(x => x.ID_curs != c.ID_curs))
                {
                    result.Add(c);
                }
            }
        }

        var aggregates = await GetCourseRatingAggregatesAsync(cancellationToken);
        return ApplyReviewAggregatesToCourses(result, aggregates);
    }

    public async Task<bool> SetCourseTeacherAsync(int courseId, int? teacherUserId, CancellationToken cancellationToken = default)
    {
        if (courseId <= 0)
        {
            return false;
        }

        await EnsureCourseTeacherColumnAsync(cancellationToken);

        await using var connection = new OleDbConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        var courseTable = GetExistingTableName(connection, "COURSES", "COURS");
        var idColumn = GetExistingColumnName(connection, courseTable, "ID_curs", "ID_cours");
        var teacherCol = TryGetExistingColumnName(connection, courseTable, "teacher_user_id", "ID_teacher", "teacher_id", "teacher");
        if (teacherCol is null)
        {
            return false;
        }

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $"UPDATE [{courseTable}] SET [{teacherCol}] = ? WHERE [{idColumn}] = ?";
        cmd.Parameters.AddWithValue("@p1", teacherUserId.HasValue ? teacherUserId.Value : (object)DBNull.Value);
        cmd.Parameters.AddWithValue("@p2", courseId);
        _ = await cmd.ExecuteNonQueryAsync(cancellationToken);

        // Проверяем, что значение реально записалось (Access/OleDb иногда возвращает странные affected).
        await using var verify = connection.CreateCommand();
        verify.CommandText = $"SELECT [{teacherCol}] FROM [{courseTable}] WHERE [{idColumn}] = ?";
        verify.Parameters.AddWithValue("@p1", courseId);
        var value = await verify.ExecuteScalarAsync(cancellationToken);
        var current = ToNullableInt(value);
        if (teacherUserId is null)
        {
            return current is null;
        }
        return current == teacherUserId.Value;
    }

    public async Task<IReadOnlyList<EnrollmentItem>> GetEnrollmentsByCourseIdAsync(int courseId, CancellationToken cancellationToken = default)
    {
        var result = new List<EnrollmentItem>();
        await using var connection = new OleDbConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        var enrollmentTable = GetExistingTableName(connection, "ENROLLMENTS");
        var enrollmentIdColumn = GetExistingColumnName(connection, enrollmentTable, "ID_enrolment");
        var enrollmentCourseColumn = GetEnrollmentCourseColumn(connection, enrollmentTable);
        var enrollmentUserColumn = GetExistingColumnName(connection, enrollmentTable, "ID_user");
        var enrollmentPayMethodColumn = TryGetExistingColumnName(connection, enrollmentTable, "PAY_method");
        var enrollmentStatusColumn = TryGetEnrollmentStatusColumn(connection, enrollmentTable);
        var enrollmentPriceColumn = TryGetExistingColumnName(connection, enrollmentTable, "price");

        var payMethodSelect = enrollmentPayMethodColumn is null ? "''" : $"[{enrollmentPayMethodColumn}]";
        var statusSelect = enrollmentStatusColumn is null ? "''" : $"[{enrollmentStatusColumn}]";
        var priceSelect = enrollmentPriceColumn is null ? "0" : $"[{enrollmentPriceColumn}]";

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $"""
            SELECT
                [{enrollmentIdColumn}] AS enrollment_id,
                [{enrollmentCourseColumn}] AS enrollment_course_id,
                [{enrollmentUserColumn}] AS enrollment_user_id,
                {payMethodSelect} AS enrollment_pay_method,
                {statusSelect} AS enrollment_status,
                {priceSelect} AS enrollment_price
            FROM [{enrollmentTable}]
            WHERE [{enrollmentCourseColumn}] = ?
            ORDER BY [{enrollmentIdColumn}] DESC
            """;
        cmd.Parameters.AddWithValue("@p1", courseId);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        if (reader is null)
        {
            return result;
        }

        while (await reader.ReadAsync(cancellationToken))
        {
            result.Add(new EnrollmentItem
            {
                ID_enrolment = ToInt(reader["enrollment_id"]),
                ID_cours = ToInt(reader["enrollment_course_id"]),
                ID_user = ToInt(reader["enrollment_user_id"]),
                PAY_method = ToStringSafe(reader["enrollment_pay_method"]),
                status = ToStringSafe(reader["enrollment_status"]),
                price = ToDecimal(reader["enrollment_price"])
            });
        }

        return result;
    }

    public async Task DeleteCourseAdminAsync(int courseId, CancellationToken cancellationToken = default)
    {
        if (courseId <= 0) return;
        await using var connection = new OleDbConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        var courseTable = GetExistingTableName(connection, "COURSES", "COURS");
        var idColumn = GetExistingColumnName(connection, courseTable, "ID_curs", "ID_cours");

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $"DELETE FROM [{courseTable}] WHERE [{idColumn}] = ?";
        cmd.Parameters.AddWithValue("@p1", courseId);
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task<int> GetNextIdAsync(OleDbConnection connection, string table, string idColumn, CancellationToken cancellationToken)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $"SELECT MAX([{idColumn}]) FROM [{table}]";
        var v = await cmd.ExecuteScalarAsync(cancellationToken);
        return Math.Max(0, ToInt(v)) + 1;
    }

    public async Task<int> CreateEnrollmentAsync(
        int userId,
        int courseId,
        string payMethod,
        decimal price,
        CancellationToken cancellationToken = default)
    {
        await using var connection = new OleDbConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        var enrollmentTable = GetExistingTableName(connection, "ENROLLMENTS");
        var enrollmentIdColumn = GetExistingColumnName(connection, enrollmentTable, "ID_enrolment");
        var enrollmentCourseColumn = GetEnrollmentCourseColumn(connection, enrollmentTable);
        var enrollmentUserColumn = GetExistingColumnName(connection, enrollmentTable, "ID_user");
        var enrollmentHomeworkColumn = TryGetExistingColumnName(connection, enrollmentTable, "ID_homework");
        var enrollmentPayMethodColumn = TryGetExistingColumnName(connection, enrollmentTable, "PAY_method");
        var enrollmentStatusColumn = TryGetEnrollmentStatusColumn(connection, enrollmentTable);
        var enrollmentPriceColumn = TryGetExistingColumnName(connection, enrollmentTable, "price");

        var newEnrollmentId = await GetNextEnrollmentIdAsync(connection, enrollmentTable, enrollmentIdColumn, cancellationToken);
        var homeworkId = await GetHomeworkIdForCourseAsync(courseId, cancellationToken) ?? 0;

        var columns = new List<string> { $"[{enrollmentIdColumn}]", $"[{enrollmentCourseColumn}]", $"[{enrollmentUserColumn}]" };
        var values = new List<object?> { newEnrollmentId, courseId, userId };
        if (enrollmentHomeworkColumn is not null)
        {
            columns.Add($"[{enrollmentHomeworkColumn}]");
            values.Add(homeworkId);
        }
        if (enrollmentPayMethodColumn is not null)
        {
            columns.Add($"[{enrollmentPayMethodColumn}]");
            values.Add(payMethod);
        }
        if (enrollmentStatusColumn is not null)
        {
            columns.Add($"[{enrollmentStatusColumn}]");
            values.Add("active");
        }
        if (enrollmentPriceColumn is not null)
        {
            columns.Add($"[{enrollmentPriceColumn}]");
            values.Add(price);
        }

        await using var command = connection.CreateCommand();
        command.CommandText = $"INSERT INTO [{enrollmentTable}] ({string.Join(", ", columns)}) VALUES ({string.Join(", ", Enumerable.Repeat("?", columns.Count))})";
        for (var i = 0; i < values.Count; i++)
        {
            command.Parameters.AddWithValue($"@p{i + 1}", values[i] ?? DBNull.Value);
        }

        await command.ExecuteNonQueryAsync(cancellationToken);
        return newEnrollmentId;
    }

    public async Task<int> CreatePaymentRecordAsync(
        int userId,
        int courseId,
        int enrollmentId,
        int homeworkId = 0,
        string? payMethod = null,
        decimal? price = null,
        string? status = null,
        CancellationToken cancellationToken = default)
    {
        await using var connection = new OleDbConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        var payTable = GetExistingTableName(connection, "PAY");
        var payUserColumn = GetExistingColumnName(connection, payTable, "ID_user");
        var payCourseColumn = GetExistingColumnName(connection, payTable, "ID_cours", "ID_curs");
        var payEnrollmentColumn = GetExistingColumnName(connection, payTable, "ID_enrolment");
        var payHomeworkColumn = TryGetExistingColumnName(connection, payTable, "ID_homework");
        var payMethodColumn = TryGetExistingColumnName(connection, payTable, "PAY_method");
        var payStatusColumn = TryGetExistingColumnName(connection, payTable, "status");
        var payPriceColumn = TryGetExistingColumnName(connection, payTable, "price");

        var columns = new List<string> { $"[{payEnrollmentColumn}]", $"[{payCourseColumn}]", $"[{payUserColumn}]" };
        var values = new List<object?> { enrollmentId, courseId, userId };
        if (payHomeworkColumn is not null)
        {
            columns.Add($"[{payHomeworkColumn}]");
            values.Add(homeworkId);
        }
        if (payMethodColumn is not null)
        {
            columns.Add($"[{payMethodColumn}]");
            values.Add(string.IsNullOrWhiteSpace(payMethod) ? "local" : payMethod);
        }
        if (payStatusColumn is not null)
        {
            columns.Add($"[{payStatusColumn}]");
            values.Add(string.IsNullOrWhiteSpace(status) ? "completed" : status);
        }
        if (payPriceColumn is not null)
        {
            columns.Add($"[{payPriceColumn}]");
            values.Add(price ?? 0m);
        }

        await using var command = connection.CreateCommand();
        command.CommandText = $"INSERT INTO [{payTable}] ({string.Join(", ", columns)}) VALUES ({string.Join(", ", Enumerable.Repeat("?", columns.Count))})";
        for (var i = 0; i < values.Count; i++)
        {
            command.Parameters.AddWithValue($"@p{i + 1}", values[i] ?? DBNull.Value);
        }

        _ = await command.ExecuteNonQueryAsync(cancellationToken);
        return enrollmentId;
    }

    /// <summary>
    /// Заполняет PAY по существующим ENROLLMENTS (если PAY пуст/неполон).
    /// Нужен для честной админ-статистики и для баз, где ранее запись в PAY не создавалась.
    /// </summary>
    public async Task BackfillPayFromEnrollmentsAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = new OleDbConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var enrollmentTable = TryGetExistingTableName(connection, "ENROLLMENTS");
        var payTable = TryGetExistingTableName(connection, "PAY");
        if (string.IsNullOrWhiteSpace(enrollmentTable) || string.IsNullOrWhiteSpace(payTable))
        {
            return;
        }

        var enrollmentIdColumn = GetExistingColumnName(connection, enrollmentTable, "ID_enrolment");
        var enrollmentCourseColumn = GetEnrollmentCourseColumn(connection, enrollmentTable);
        var enrollmentUserColumn = GetExistingColumnName(connection, enrollmentTable, "ID_user");
        var enrollmentPayMethodColumn = TryGetExistingColumnName(connection, enrollmentTable, "PAY_method");
        var enrollmentStatusColumn = TryGetEnrollmentStatusColumn(connection, enrollmentTable);
        var enrollmentPriceColumn = TryGetExistingColumnName(connection, enrollmentTable, "price");

        var payUserColumn = GetExistingColumnName(connection, payTable, "ID_user");
        var payCourseColumn = GetExistingColumnName(connection, payTable, "ID_cours", "ID_curs");
        var payEnrollmentColumn = GetExistingColumnName(connection, payTable, "ID_enrolment");
        var payHomeworkColumn = TryGetExistingColumnName(connection, payTable, "ID_homework");
        var payMethodColumn = TryGetExistingColumnName(connection, payTable, "PAY_method");
        var payStatusColumn = TryGetExistingColumnName(connection, payTable, "status");
        var payPriceColumn = TryGetExistingColumnName(connection, payTable, "price");

        var enrollmentPayMethodSelect = enrollmentPayMethodColumn is null ? "''" : $"[{enrollmentPayMethodColumn}]";
        var enrollmentStatusSelect = enrollmentStatusColumn is null ? "''" : $"[{enrollmentStatusColumn}]";
        var enrollmentPriceSelect = enrollmentPriceColumn is null ? "0" : $"[{enrollmentPriceColumn}]";

        await using var readEnrollments = connection.CreateCommand();
        readEnrollments.CommandText = $"""
            SELECT [{enrollmentIdColumn}] AS e_id,
                   [{enrollmentCourseColumn}] AS e_course_id,
                   [{enrollmentUserColumn}] AS e_user_id,
                   {enrollmentPayMethodSelect} AS e_method,
                   {enrollmentStatusSelect} AS e_status,
                   {enrollmentPriceSelect} AS e_price
            FROM [{enrollmentTable}]
            ORDER BY [{enrollmentIdColumn}] DESC
            """;

        await using var reader = await readEnrollments.ExecuteReaderAsync(cancellationToken);
        if (reader is null)
        {
            return;
        }

        while (await reader.ReadAsync(cancellationToken))
        {
            var enrollmentId = ToInt(reader["e_id"]);
            var courseId = ToInt(reader["e_course_id"]);
            var userId = ToInt(reader["e_user_id"]);
            if (enrollmentId <= 0 || courseId <= 0 || userId <= 0)
            {
                continue;
            }

            var hasPayRecord = false;
            await using (var checkPay = connection.CreateCommand())
            {
                checkPay.CommandText = $"""
                    SELECT COUNT(*)
                    FROM [{payTable}]
                    WHERE [{payEnrollmentColumn}] = ?
                    """;
                checkPay.Parameters.AddWithValue("@p1", enrollmentId);
                hasPayRecord = ToInt(await checkPay.ExecuteScalarAsync(cancellationToken)) > 0;
            }
            if (hasPayRecord)
            {
                continue;
            }

            var columns = new List<string> { $"[{payEnrollmentColumn}]", $"[{payCourseColumn}]", $"[{payUserColumn}]" };
            var values = new List<object?> { enrollmentId, courseId, userId };

            if (payHomeworkColumn is not null)
            {
                // Исторически PAY мог требовать ID_homework. Берём первое ДЗ курса, если есть.
                var hw = await GetHomeworkIdForCourseAsync(courseId, cancellationToken) ?? 0;
                columns.Add($"[{payHomeworkColumn}]");
                values.Add(hw);
            }
            if (payMethodColumn is not null)
            {
                columns.Add($"[{payMethodColumn}]");
                values.Add(ToStringSafe(reader["e_method"]));
            }
            if (payStatusColumn is not null)
            {
                columns.Add($"[{payStatusColumn}]");
                values.Add(ToStringSafe(reader["e_status"]));
            }
            if (payPriceColumn is not null)
            {
                columns.Add($"[{payPriceColumn}]");
                values.Add(ToDecimal(reader["e_price"]));
            }

            try
            {
                await using var insertPay = connection.CreateCommand();
                insertPay.CommandText = $"INSERT INTO [{payTable}] ({string.Join(", ", columns)}) VALUES ({string.Join(", ", Enumerable.Repeat("?", columns.Count))})";
                for (var i = 0; i < values.Count; i++)
                {
                    insertPay.Parameters.AddWithValue($"@p{i + 1}", values[i] ?? DBNull.Value);
                }
                _ = await insertPay.ExecuteNonQueryAsync(cancellationToken);
            }
            catch (OleDbException)
            {
                // Если в конкретной БД PAY связан жёсткими внешними ключами/ограничениями — не ломаем страницу админа.
            }
        }
    }

    /// <summary>
    /// Чинит "нулевые цены" и пустой метод оплаты в ENROLLMENTS, подставляя цену курса и метод local.
    /// Делает это аккуратно: только если price <= 0 или PAY_method пустой.
    /// </summary>
    public async Task BackfillEnrollmentPaymentFieldsAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = new OleDbConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var enrollmentTable = TryGetExistingTableName(connection, "ENROLLMENTS");
        var coursesTable = TryGetExistingTableName(connection, "COURSES", "COURS");
        if (string.IsNullOrWhiteSpace(enrollmentTable) || string.IsNullOrWhiteSpace(coursesTable))
        {
            return;
        }

        var enrollmentIdColumn = GetExistingColumnName(connection, enrollmentTable, "ID_enrolment");
        var enrollmentCourseColumn = GetEnrollmentCourseColumn(connection, enrollmentTable);
        var enrollmentPayMethodColumn = TryGetExistingColumnName(connection, enrollmentTable, "PAY_method");
        var enrollmentPriceColumn = TryGetExistingColumnName(connection, enrollmentTable, "price");
        if (enrollmentPayMethodColumn is null && enrollmentPriceColumn is null)
        {
            return;
        }

        var courseIdColumn = GetExistingColumnName(connection, coursesTable, "ID_curs", "ID_cours");
        var coursePriceColumn = TryGetExistingColumnName(connection, coursesTable, "price", "Price");
        if (coursePriceColumn is null)
        {
            return;
        }

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $"""
            SELECT e.[{enrollmentIdColumn}] AS e_id,
                   e.[{enrollmentCourseColumn}] AS e_course_id,
                   {(enrollmentPayMethodColumn is null ? "''" : $"e.[{enrollmentPayMethodColumn}]")} AS e_method,
                   {(enrollmentPriceColumn is null ? "0" : $"e.[{enrollmentPriceColumn}]")} AS e_price,
                   c.[{coursePriceColumn}] AS c_price
            FROM [{enrollmentTable}] AS e
            LEFT JOIN [{coursesTable}] AS c ON c.[{courseIdColumn}] = e.[{enrollmentCourseColumn}]
            """;

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        if (reader is null)
        {
            return;
        }

        while (await reader.ReadAsync(cancellationToken))
        {
            var enrollmentId = ToInt(reader["e_id"]);
            if (enrollmentId <= 0)
            {
                continue;
            }

            var currentMethod = ToStringSafe(reader["e_method"]);
            var currentPrice = ToDecimal(reader["e_price"]);
            var coursePrice = ToDecimal(reader["c_price"]);

            var needMethod = enrollmentPayMethodColumn is not null && string.IsNullOrWhiteSpace(currentMethod);
            var needPrice = enrollmentPriceColumn is not null && currentPrice <= 0 && coursePrice > 0;
            if (!needMethod && !needPrice)
            {
                continue;
            }

            var sets = new List<string>();
            var values = new List<object?>();
            if (needMethod && enrollmentPayMethodColumn is not null)
            {
                sets.Add($"[{enrollmentPayMethodColumn}] = ?");
                values.Add("local");
            }
            if (needPrice && enrollmentPriceColumn is not null)
            {
                sets.Add($"[{enrollmentPriceColumn}] = ?");
                values.Add(coursePrice);
            }

            if (sets.Count == 0)
            {
                continue;
            }

            await using var upd = connection.CreateCommand();
            upd.CommandText = $"UPDATE [{enrollmentTable}] SET {string.Join(", ", sets)} WHERE [{enrollmentIdColumn}] = ?";
            for (var i = 0; i < values.Count; i++)
            {
                upd.Parameters.AddWithValue($"@p{i + 1}", values[i] ?? DBNull.Value);
            }
            upd.Parameters.AddWithValue($"@p{values.Count + 1}", enrollmentId);
            _ = await upd.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    public async Task<int?> GetHomeworkIdForCourseAsync(int courseId, CancellationToken cancellationToken = default)
    {
        var courseLessons = await GetLessonsByCourseIdAsync(courseId, cancellationToken);
        var lessonIds = courseLessons.Select(l => l.ID_lesson).Where(x => x > 0).ToList();

        await using var connection = new OleDbConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var homeworkTable = GetExistingTableName(connection, "HOMEWORK", "PROGRES");

        foreach (var lessonId in lessonIds)
        {
            await using var hwCommand = connection.CreateCommand();
            hwCommand.CommandText = $"""
                SELECT TOP 1 ID_homework
                FROM [{homeworkTable}]
                WHERE ID_lesson = ?
                ORDER BY ID_homework
                """;
            hwCommand.Parameters.AddWithValue("@p1", lessonId);

            var value = await hwCommand.ExecuteScalarAsync(cancellationToken);
            var homeworkId = ToInt(value);
            if (homeworkId > 0)
            {
                return homeworkId;
            }
        }

        return null;
    }

    private async Task<int?> GetAnyHomeworkIdAsync(OleDbConnection connection, CancellationToken cancellationToken, OleDbTransaction? transaction = null)
    {
        var homeworkTable = TryGetExistingTableName(connection, "HOMEWORK", "PROGRES");
        if (string.IsNullOrWhiteSpace(homeworkTable))
        {
            return null;
        }

        var hwIdCol = TryGetExistingColumnName(connection, homeworkTable, "ID_homework");
        if (hwIdCol is null)
        {
            return null;
        }

        await using var cmd = connection.CreateCommand();
        cmd.Transaction = transaction;
        cmd.CommandText = $"SELECT TOP 1 [{hwIdCol}] FROM [{homeworkTable}] ORDER BY [{hwIdCol}]";
        var v = await cmd.ExecuteScalarAsync(cancellationToken);
        var id = ToInt(v);
        return id > 0 ? id : null;
    }

    public async Task<decimal> GetTotalPaidRevenueRobustAsync(CancellationToken cancellationToken = default)
    {
        // Надёжный расчёт в C#: не зависит от капризов Access SQL и типов колонок.
        var enrollments = await GetAllEnrollmentsAsync(cancellationToken);
        var payments = await GetAllPaymentsAsync(cancellationToken);
        var courses = await GetCoursesAsync(cancellationToken);

        var coursePriceById = courses.ToDictionary(c => c.ID_curs, c => c.price);
        var paymentsByEnrollment = payments
            .Where(p => p.ID_enrolment > 0)
            .GroupBy(p => p.ID_enrolment)
            .ToDictionary(g => g.Key, g => g.First());

        decimal total = 0m;
        foreach (var e in enrollments)
        {
            var st = (e.status ?? string.Empty).Trim();
            var stNorm = st.ToLowerInvariant();
            // cancelled не учитываем в прибыли; refund_requested считаем до решения админа.
            if (stNorm is "cancelled")
            {
                continue;
            }

            var hasPaymentMark =
                paymentsByEnrollment.ContainsKey(e.ID_enrolment) ||
                string.Equals(st, "completed", StringComparison.OrdinalIgnoreCase);
            if (!hasPaymentMark)
            {
                continue;
            }

            decimal amount = 0m;
            if (paymentsByEnrollment.TryGetValue(e.ID_enrolment, out var pay))
            {
                var payStatus = (pay.status ?? string.Empty).Trim().ToLowerInvariant();
                if (payStatus == "refunded")
                {
                    continue;
                }

                if (pay.price > 0)
                {
                    amount = pay.price;
                }
            }

            if (amount <= 0 && e.price > 0)
            {
                amount = e.price;
            }

            if (amount <= 0 && coursePriceById.TryGetValue(e.ID_cours, out var coursePrice) && coursePrice > 0)
            {
                amount = coursePrice;
            }

            total += amount;
        }

        return total;
    }

    public async Task EnsureEnrollmentAndPaymentAsync(
        int userId,
        int courseId,
        string payMethod,
        decimal price,
        int coinsToSpend = 0,
        CancellationToken cancellationToken = default)
    {
        // Чтобы корректно отличать отменённые записи и создавать новую подписку после возврата,
        // гарантируем наличие статуса в ENROLLMENTS.
        await EnsureEnrollmentStatusColumnAsync(cancellationToken);
        await EnsureUserRubBalanceColumnAsync(cancellationToken);

        await using var connection = new OleDbConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        using var transaction = connection.BeginTransaction();

        try
        {
            var enrollmentTable = GetExistingTableName(connection, "ENROLLMENTS");
            var enrollmentIdColumn = GetExistingColumnName(connection, enrollmentTable, "ID_enrolment", "ID_enrollment", "enrollment_id");
            var enrollmentCourseColumn = GetEnrollmentCourseColumn(connection, enrollmentTable);
            var enrollmentUserColumn = GetExistingColumnName(connection, enrollmentTable, "ID_user", "user_id", "ID_users");
            var enrollmentHomeworkColumn = TryGetExistingColumnName(connection, enrollmentTable, "ID_homework");
            var enrollmentPayMethodColumn = TryGetExistingColumnName(connection, enrollmentTable, "PAY_method");
            var enrollmentStatusColumn = TryGetEnrollmentStatusColumn(connection, enrollmentTable);
            var enrollmentPriceColumn = TryGetExistingColumnName(connection, enrollmentTable, "price");

            var existingEnrollmentId = 0;
            var needsEnrollmentInsert = false;
            var existingStatus = string.Empty;
            await using (var checkEnrollment = connection.CreateCommand())
            {
                checkEnrollment.Transaction = transaction;
                checkEnrollment.CommandText = $"""
                    SELECT TOP 1
                        [{enrollmentIdColumn}] AS e_id,
                        {(enrollmentStatusColumn is null ? "''" : $"[{enrollmentStatusColumn}]")} AS e_status
                    FROM [{enrollmentTable}]
                    WHERE [{enrollmentUserColumn}] = ? AND [{enrollmentCourseColumn}] = ?
                    ORDER BY [{enrollmentIdColumn}] DESC
                    """;
                checkEnrollment.Parameters.AddWithValue("@p1", userId);
                checkEnrollment.Parameters.AddWithValue("@p2", courseId);
                await using var r = await checkEnrollment.ExecuteReaderAsync(cancellationToken);
                if (r is not null && await r.ReadAsync(cancellationToken))
                {
                    existingEnrollmentId = ToInt(r["e_id"]);
                    existingStatus = ToStringSafe(r["e_status"]).Trim();
                }
            }

            var stNorm = existingStatus.Trim().ToLowerInvariant();
            var shouldReuseExisting = existingEnrollmentId > 0 &&
                                     stNorm is not "cancelled" and not "refund_requested";

            // Если последняя запись отменена — создаём новую (иначе пользователь "оплачивает", но остаётся cancelled).
            if (!shouldReuseExisting)
            {
                existingEnrollmentId = await GetNextEnrollmentIdAsync(connection, enrollmentTable, enrollmentIdColumn, cancellationToken, transaction);
                needsEnrollmentInsert = true;
            }

            var schemaTables = connection.GetSchema("Tables");
            var hasPayTable = false;
            foreach (DataRow row in schemaTables.Rows)
            {
                var tableType = row["TABLE_TYPE"]?.ToString();
                var tableName = row["TABLE_NAME"]?.ToString();
                if (string.Equals(tableType, "TABLE", StringComparison.OrdinalIgnoreCase) &&
                    string.Equals(tableName, "PAY", StringComparison.OrdinalIgnoreCase))
                {
                    hasPayTable = true;
                    break;
                }
            }

            // Важно: если мы только что создаём запись ENROLLMENTS, сначала вставляем её,
            // иначе вставка в PAY может упасть на внешних ключах/связях.
            var userTable = GetExistingTableName(connection, "USER", "USERS");
            var userIdColumn = GetExistingColumnName(connection, userTable, "ID_user", "user_id");
            var userCourseColumn = GetExistingColumnName(connection, userTable, "ID_curs", "ID_cours", "course_id");
            var userBonusColumn = TryGetExistingColumnName(connection, userTable, "balanse_coins", "balance_coins", "coins_balance");
            var currentCoins = 0;
            if (userBonusColumn is not null)
            {
                await using var readCoins = connection.CreateCommand();
                readCoins.Transaction = transaction;
                readCoins.CommandText = $"SELECT [{userBonusColumn}] FROM [{userTable}] WHERE [{userIdColumn}] = ?";
                readCoins.Parameters.AddWithValue("@p1", userId);
                currentCoins = Math.Max(0, ToInt(await readCoins.ExecuteScalarAsync(cancellationToken)));
            }
            var maxCoinsByPrice = (int)Math.Floor(Math.Max(0m, price));
            var effectiveCoinsToSpend = Math.Max(0, Math.Min(Math.Min(coinsToSpend, currentCoins), maxCoinsByPrice));
            var paidAmount = Math.Max(0m, price - effectiveCoinsToSpend);

            if (needsEnrollmentInsert)
            {
                var homeworkIdForEnrollment = await GetHomeworkIdForCourseAsync(courseId, cancellationToken)
                    ?? await GetAnyHomeworkIdAsync(connection, cancellationToken, transaction)
                    ?? 0;
                var columns = new List<string> { $"[{enrollmentIdColumn}]", $"[{enrollmentCourseColumn}]", $"[{enrollmentUserColumn}]" };
                var values = new List<object?> { existingEnrollmentId, courseId, userId };
                if (enrollmentHomeworkColumn is not null)
                {
                    columns.Add($"[{enrollmentHomeworkColumn}]");
                    values.Add(homeworkIdForEnrollment > 0 ? homeworkIdForEnrollment : DBNull.Value);
                }
                if (enrollmentPayMethodColumn is not null)
                {
                    columns.Add($"[{enrollmentPayMethodColumn}]");
                    values.Add(payMethod);
                }
                if (enrollmentStatusColumn is not null)
                {
                    columns.Add($"[{enrollmentStatusColumn}]");
                    values.Add("active");
                }
                if (enrollmentPriceColumn is not null)
                {
                    columns.Add($"[{enrollmentPriceColumn}]");
                    values.Add(paidAmount);
                }

                await using var insertEnrollment = connection.CreateCommand();
                insertEnrollment.Transaction = transaction;
                insertEnrollment.CommandText = $"INSERT INTO [{enrollmentTable}] ({string.Join(", ", columns)}) VALUES ({string.Join(", ", Enumerable.Repeat("?", columns.Count))})";
                for (var i = 0; i < values.Count; i++)
                {
                    insertEnrollment.Parameters.AddWithValue($"@p{i + 1}", values[i] ?? DBNull.Value);
                }
                await insertEnrollment.ExecuteNonQueryAsync(cancellationToken);
            }

            var paymentCreated = false;
            if (hasPayTable)
            {
                var payTable = GetExistingTableName(connection, "PAY");
                var payUserColumn = GetExistingColumnName(connection, payTable, "ID_user", "user_id", "ID_users");
                var payCourseColumn = GetExistingColumnName(connection, payTable, "ID_cours", "ID_curs", "course_id", "ID_course");
                var payEnrollmentColumn = GetExistingColumnName(connection, payTable, "ID_enrolment", "ID_enrollment", "enrollment_id");
                var payHomeworkColumn = TryGetExistingColumnName(connection, payTable, "ID_homework");
                var payMethodColumn = TryGetExistingColumnName(connection, payTable, "PAY_method");
                var payStatusColumn = TryGetExistingColumnName(connection, payTable, "status", "State", "pay_status");
                var payPriceColumn = TryGetExistingColumnName(connection, payTable, "price");

                var hasPayRecord = false;
                await using (var checkPay = connection.CreateCommand())
                {
                    checkPay.Transaction = transaction;
                    checkPay.CommandText = $"""
                        SELECT COUNT(*)
                        FROM [{payTable}]
                        WHERE [{payEnrollmentColumn}] = ?
                        """;
                    checkPay.Parameters.AddWithValue("@p1", existingEnrollmentId);
                    var countObj = await checkPay.ExecuteScalarAsync(cancellationToken);
                    hasPayRecord = ToInt(countObj) > 0;
                }

                if (!hasPayRecord)
                {
                    try
                    {
                        var columns = new List<string> { $"[{payEnrollmentColumn}]", $"[{payCourseColumn}]", $"[{payUserColumn}]" };
                        var values = new List<object?> { existingEnrollmentId, courseId, userId };
                        if (payHomeworkColumn is not null)
                        {
                            var homeworkId = await GetHomeworkIdForCourseAsync(courseId, cancellationToken)
                                ?? await GetAnyHomeworkIdAsync(connection, cancellationToken, transaction)
                                ?? 0;
                            columns.Add($"[{payHomeworkColumn}]");
                            values.Add(homeworkId > 0 ? homeworkId : DBNull.Value);
                        }
                        if (payMethodColumn is not null)
                        {
                            columns.Add($"[{payMethodColumn}]");
                            values.Add(payMethod);
                        }
                        if (payStatusColumn is not null)
                        {
                            columns.Add($"[{payStatusColumn}]");
                            values.Add("completed");
                        }
                        if (payPriceColumn is not null)
                        {
                            columns.Add($"[{payPriceColumn}]");
                            values.Add(paidAmount);
                        }

                        await using var insertPay = connection.CreateCommand();
                        insertPay.Transaction = transaction;
                        insertPay.CommandText = $"INSERT INTO [{payTable}] ({string.Join(", ", columns)}) VALUES ({string.Join(", ", Enumerable.Repeat("?", columns.Count))})";
                        for (var i = 0; i < values.Count; i++)
                        {
                            insertPay.Parameters.AddWithValue($"@p{i + 1}", values[i] ?? DBNull.Value);
                        }
                        _ = await insertPay.ExecuteNonQueryAsync(cancellationToken);
                        paymentCreated = true;
                    }
                    catch (OleDbException)
                    {
                        // If PAY has strict relations in a specific deployment, keep enrollment and user update committed.
                    }
                }
            }

            // Пустой статус после оплаты или повторная запись после отмены — «active» (не «completed»: это путалось с «курс сдан»).
            if (!needsEnrollmentInsert && enrollmentStatusColumn is not null &&
                (string.IsNullOrWhiteSpace(existingStatus) || stNorm is "cancelled" or "refund_requested"))
            {
                await using var fixStatus = connection.CreateCommand();
                fixStatus.Transaction = transaction;
                fixStatus.CommandText = $"""
                    UPDATE [{enrollmentTable}]
                    SET [{enrollmentStatusColumn}] = ?
                    WHERE [{enrollmentIdColumn}] = ?
                    """;
                fixStatus.Parameters.AddWithValue("@p1", "active");
                fixStatus.Parameters.AddWithValue("@p2", existingEnrollmentId);
                _ = await fixStatus.ExecuteNonQueryAsync(cancellationToken);
            }

            // Раньше статус «completed» означал «оплачено» и попадал в «завершённые курсы» — нормализуем в «active».
            if (!needsEnrollmentInsert && enrollmentStatusColumn is not null && stNorm == "completed")
            {
                await using var normLegacy = connection.CreateCommand();
                normLegacy.Transaction = transaction;
                normLegacy.CommandText = $"""
                    UPDATE [{enrollmentTable}]
                    SET [{enrollmentStatusColumn}] = ?
                    WHERE [{enrollmentIdColumn}] = ?
                    """;
                normLegacy.Parameters.AddWithValue("@p1", "active");
                normLegacy.Parameters.AddWithValue("@p2", existingEnrollmentId);
                _ = await normLegacy.ExecuteNonQueryAsync(cancellationToken);
            }

            await using (var updateUser = connection.CreateCommand())
            {
                updateUser.Transaction = transaction;
                var bonusAward = (int)Math.Floor(Math.Max(0m, paidAmount) * 0.03m); // 3% от реально оплаченной суммы

                var sets = new List<string> { $"[{userCourseColumn}] = ?" };
                var values = new List<object?> { courseId };
                if ((needsEnrollmentInsert || paymentCreated) && userBonusColumn is not null)
                {
                    var nextCoins = Math.Max(0, currentCoins - effectiveCoinsToSpend + bonusAward);
                    sets.Add($"[{userBonusColumn}] = ?");
                    values.Add(nextCoins);
                }

                updateUser.CommandText = $"UPDATE [{userTable}] SET {string.Join(", ", sets)} WHERE [{userIdColumn}] = ?";
                for (var i = 0; i < values.Count; i++)
                {
                    updateUser.Parameters.AddWithValue($"@p{i + 1}", values[i] ?? DBNull.Value);
                }
                updateUser.Parameters.AddWithValue($"@p{values.Count + 1}", userId);
                await updateUser.ExecuteNonQueryAsync(cancellationToken);
            }

            transaction.Commit();
        }
        catch
        {
            transaction.Rollback();
            throw;
        }
    }

    public async Task<IReadOnlyList<CourseItem>> GetPurchasedCoursesByUserIdAsync(int userId, CancellationToken cancellationToken = default)
    {
        var courseIds = new HashSet<int>();
        var excludedCourseIds = new HashSet<int>();

        try
        {
            var enrollments = await GetEnrollmentsByUserIdAsync(userId, cancellationToken);
            foreach (var enrollment in enrollments)
            {
                var st = (enrollment.status ?? string.Empty).Trim().ToLowerInvariant();
                if (st is "cancelled")
                {
                    if (enrollment.ID_cours > 0)
                    {
                        excludedCourseIds.Add(enrollment.ID_cours);
                    }
                    continue;
                }
                if (enrollment.ID_cours > 0)
                {
                    courseIds.Add(enrollment.ID_cours);
                }
            }
        }
        catch
        {
            // fallback to PAY below
        }

        await using (var connection = new OleDbConnection(_connectionString))
        {
            await connection.OpenAsync(CancellationToken.None);
            var payTable = GetExistingTableName(connection, "PAY");
            var payUserColumn = GetExistingColumnName(connection, payTable, "ID_user", "user_id");
            var payCourseColumn = GetExistingColumnName(connection, payTable, "ID_curs", "ID_cours", "course_id");

            await using var payCommand = connection.CreateCommand();
            payCommand.CommandText = $"""
                SELECT [{payCourseColumn}] AS pay_course_id
                FROM [{payTable}]
                WHERE [{payUserColumn}] = ?
                """;
            payCommand.Parameters.AddWithValue("@p1", userId);

            await using var reader = await payCommand.ExecuteReaderAsync(CancellationToken.None);
            if (reader is not null)
            {
                while (await reader.ReadAsync(CancellationToken.None))
                {
                    var courseId = ToInt(reader["pay_course_id"]);
                    if (courseId > 0 && !excludedCourseIds.Contains(courseId))
                    {
                        courseIds.Add(courseId);
                    }
                }
            }
        }

        var result = new List<CourseItem>();
        foreach (var courseId in courseIds)
        {
            var course = await GetCourseByIdAsync(courseId, CancellationToken.None);
            if (course is not null)
            {
                result.Add(course);
            }
        }

        return result.OrderBy(c => c.Course_name).ToList();
    }

    public async Task<bool> ApproveRefundForEnrollmentAsync(int enrollmentId, int userId, int courseId, CancellationToken cancellationToken = default)
    {
        if (enrollmentId <= 0 || userId <= 0 || courseId <= 0)
        {
            return false;
        }

        await EnsureEnrollmentStatusColumnAsync(cancellationToken);
        await EnsureUserRubBalanceColumnAsync(cancellationToken);
        await EnsureEnrollmentRefundColumnsAsync(cancellationToken);

        await using var connection = new OleDbConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        using var tx = connection.BeginTransaction();
        try
        {
            var enrollmentTable = GetExistingTableName(connection, "ENROLLMENTS");
            var enrollmentIdColumn = GetExistingColumnName(connection, enrollmentTable, "ID_enrolment", "ID_enrollment", "enrollment_id");
            var enrollmentCourseColumn = GetEnrollmentCourseColumn(connection, enrollmentTable);
            var enrollmentUserColumn = GetExistingColumnName(connection, enrollmentTable, "ID_user", "user_id", "ID_users");
            var statusColumns = GetEnrollmentStatusColumns(connection, enrollmentTable);
            var enrollmentPriceColumn = TryGetExistingColumnName(connection, enrollmentTable, "price", "Price");
            var refundProcessedColumn = TryGetEnrollmentRefundProcessedColumn(connection, enrollmentTable);
            var refundAmountColumn = TryGetEnrollmentRefundAmountColumn(connection, enrollmentTable);
            if (statusColumns.Count == 0)
            {
                throw new InvalidOperationException($"ENROLLMENTS: не удалось найти колонку статуса (ожидались: status/State/enrollment_status/learning_status).");
            }

            // Находим "главную" заявку: самую свежую строку по (user+course),
            // где хотя бы в одной статус-колонке стоит refund_requested/cancelled.
            // В старых данных могут быть дубли; ниже статусы почистим у всех таких строк, но возврат сделаем один раз.
            var whereAnyRequested = string.Join(" OR ", statusColumns.Select(c => $"LCase(Trim([{c}]))='refund_requested' OR LCase(Trim([{c}]))='cancelled'"));
            var selectedEnrollmentId = 0;
            await using (var pick = connection.CreateCommand())
            {
                pick.Transaction = tx;
                pick.CommandText = $"""
                    SELECT TOP 1 [{enrollmentIdColumn}]
                    FROM [{enrollmentTable}]
                    WHERE [{enrollmentUserColumn}] = ? AND [{enrollmentCourseColumn}] = ?
                      AND ({whereAnyRequested})
                    ORDER BY [{enrollmentIdColumn}] DESC
                    """;
                pick.Parameters.AddWithValue("@p1", userId);
                pick.Parameters.AddWithValue("@p2", courseId);
                selectedEnrollmentId = ToInt(await pick.ExecuteScalarAsync(cancellationToken));
            }
            if (selectedEnrollmentId <= 0)
            {
                tx.Rollback();
                return false;
            }

            // Если возврат уже был проведён хотя бы по одной строке этого (user+course) — не начисляем деньги повторно.
            // Но статусы всё равно зачистим (ниже).
            if (refundProcessedColumn is not null || refundAmountColumn is not null)
            {
                var processedWhere = new List<string>();
                if (refundProcessedColumn is not null)
                {
                    processedWhere.Add($"[{refundProcessedColumn}] = True");
                }
                if (refundAmountColumn is not null)
                {
                    processedWhere.Add($"[{refundAmountColumn}] > 0");
                }

                if (processedWhere.Count > 0)
                {
                    await using var checkProcessed = connection.CreateCommand();
                    checkProcessed.Transaction = tx;
                    checkProcessed.CommandText = $"""
                        SELECT COUNT(*)
                        FROM [{enrollmentTable}]
                        WHERE [{enrollmentUserColumn}] = ? AND [{enrollmentCourseColumn}] = ?
                          AND ({string.Join(" OR ", processedWhere)})
                        """;
                    checkProcessed.Parameters.AddWithValue("@p1", userId);
                    checkProcessed.Parameters.AddWithValue("@p2", courseId);
                    var cnt = ToInt(await checkProcessed.ExecuteScalarAsync(cancellationToken));
                    if (cnt > 0)
                    {
                        // Статусы зачистим, а деньги второй раз не возвращаем.
                        await using (var cancelAll2 = connection.CreateCommand())
                        {
                            cancelAll2.Transaction = tx;
                            var setStatuses2 = string.Join(", ", statusColumns.Select(c => $"[{c}] = ?"));
                            cancelAll2.CommandText = $"""
                                UPDATE [{enrollmentTable}]
                                SET {setStatuses2}
                                WHERE [{enrollmentUserColumn}] = ? AND [{enrollmentCourseColumn}] = ?
                                  AND ({whereAnyRequested})
                                """;
                            var pp = 1;
                            foreach (var _ in statusColumns)
                            {
                                cancelAll2.Parameters.AddWithValue($"@p{pp++}", "cancelled");
                            }
                            cancelAll2.Parameters.AddWithValue($"@p{pp++}", userId);
                            cancelAll2.Parameters.AddWithValue($"@p{pp}", courseId);
                            _ = await cancelAll2.ExecuteNonQueryAsync(cancellationToken);
                        }

                        tx.Commit();
                        return true;
                    }
                }
            }

            await using var read = connection.CreateCommand();
            read.Transaction = tx;
            var statusProjection = string.Join(", ", statusColumns.Select((c, i) => $"[{c}] AS e_status_{i}"));
            read.CommandText = $"""
                SELECT [{enrollmentUserColumn}] AS e_user,
                       [{enrollmentCourseColumn}] AS e_course,
                       {statusProjection},
                       {(enrollmentPriceColumn is null ? "0" : $"[{enrollmentPriceColumn}]")} AS e_price,
                       {(refundProcessedColumn is null ? "0" : $"[{refundProcessedColumn}]")} AS e_refund_processed,
                       {(refundAmountColumn is null ? "0" : $"[{refundAmountColumn}]")} AS e_refund_amount
                FROM [{enrollmentTable}]
                WHERE [{enrollmentIdColumn}] = ? AND [{enrollmentUserColumn}] = ? AND [{enrollmentCourseColumn}] = ?
                """;
            read.Parameters.AddWithValue("@p1", selectedEnrollmentId);
            read.Parameters.AddWithValue("@p2", userId);
            read.Parameters.AddWithValue("@p3", courseId);
            await using var reader = await read.ExecuteReaderAsync(cancellationToken);
            if (reader is null || !await reader.ReadAsync(cancellationToken))
            {
                throw new InvalidOperationException($"Запись ENROLLMENTS #{selectedEnrollmentId} не найдена.");
            }

            var readUserId = ToInt(reader["e_user"]);
            var readCourseId = ToInt(reader["e_course"]);
            var statusValues = new List<string>(statusColumns.Count);
            for (var i = 0; i < statusColumns.Count; i++)
            {
                statusValues.Add(ToStringSafe(reader[$"e_status_{i}"]).Trim());
            }

            static string Norm(string s) => (s ?? string.Empty).Trim().ToLowerInvariant();
            var anyRefundRequested = statusValues.Any(v => Norm(v) == "refund_requested");
            var anyCancelled = statusValues.Any(v => Norm(v) == "cancelled");
            var anyCompleted = statusValues.Any(v => Norm(v) == "completed");
            var anyFinished = statusValues.Any(v => Norm(v) == "finished");
            var enrollmentPrice = ToDecimal(reader["e_price"]);
            var alreadyProcessed = ToInt(reader["e_refund_processed"]) != 0;
            var alreadyRefundAmount = ToDecimal(reader["e_refund_amount"]);
            if (readUserId <= 0)
            {
                throw new InvalidOperationException($"ENROLLMENTS #{enrollmentId}: некорректный ID_user ({readUserId}).");
            }
            if (readCourseId <= 0)
            {
                throw new InvalidOperationException($"ENROLLMENTS #{enrollmentId}: некорректный ID курса ({readCourseId}).");
            }
            if (alreadyProcessed)
            {
                // уже проводили возврат по этой записи
                tx.Rollback();
                return true;
            }

            if (!anyRefundRequested && !anyCancelled)
            {
                var joined = string.Join(", ", statusColumns.Select((c, i) => $"{c}='{statusValues[i]}'"));
                throw new InvalidOperationException(
                    $"ENROLLMENTS #{selectedEnrollmentId}: статусы не содержат refund_requested/cancelled (найдено: {joined}).");
            }

            decimal refund = enrollmentPrice;
            if (refund <= 0)
            {
                var course = await GetCourseByIdAsync(courseId, cancellationToken);
                refund = course?.price ?? 0m;
            }
            if (alreadyRefundAmount > 0)
            {
                refund = alreadyRefundAmount;
            }

            // 1) Чистим статусы у всех дублей (user+course, где был refund_requested/cancelled).
            // 2) Но возврат денег и отметку refund_processed делаем один раз по selectedEnrollmentId.
            await using (var cancelAll = connection.CreateCommand())
            {
                cancelAll.Transaction = tx;
                var setStatuses = string.Join(", ", statusColumns.Select(c => $"[{c}] = ?"));
                cancelAll.CommandText = $"""
                    UPDATE [{enrollmentTable}]
                    SET {setStatuses}
                    WHERE [{enrollmentUserColumn}] = ? AND [{enrollmentCourseColumn}] = ?
                      AND ({whereAnyRequested})
                    """;
                var pIdx = 1;
                foreach (var _ in statusColumns)
                {
                    cancelAll.Parameters.AddWithValue($"@p{pIdx++}", "cancelled");
                }
                cancelAll.Parameters.AddWithValue($"@p{pIdx++}", userId);
                cancelAll.Parameters.AddWithValue($"@p{pIdx}", courseId);
                _ = await cancelAll.ExecuteNonQueryAsync(cancellationToken);
            }

            // Доп.проверка: после обновления статус не должен оставаться refund_requested ни в одной статус-колонке.
            await using (var verifySt = connection.CreateCommand())
            {
                verifySt.Transaction = tx;
                var selectCols = string.Join(", ", statusColumns.Select(c => $"[{c}]"));
                verifySt.CommandText = $"""
                    SELECT TOP 1 {selectCols}
                    FROM [{enrollmentTable}]
                    WHERE [{enrollmentIdColumn}] = ? AND [{enrollmentUserColumn}] = ? AND [{enrollmentCourseColumn}] = ?
                    """;
                verifySt.Parameters.AddWithValue("@p1", enrollmentId);
                verifySt.Parameters.AddWithValue("@p2", userId);
                verifySt.Parameters.AddWithValue("@p3", courseId);
                await using var vr = await verifySt.ExecuteReaderAsync(cancellationToken);
                if (vr is not null && await vr.ReadAsync(cancellationToken))
                {
                    foreach (var c in statusColumns)
                    {
                        var v = ToStringSafe(vr[c]).Trim();
                        if (string.Equals(v, "refund_requested", StringComparison.OrdinalIgnoreCase))
                        {
                            throw new InvalidOperationException($"ENROLLMENTS #{enrollmentId}: после подтверждения возврата статус всё ещё refund_requested в колонке [{c}].");
                        }
                    }
                }
            }

            // Помечаем оплату как refunded (если PAY существует) — чтобы прибыль и финансы корректно отражали возвраты.
            try
            {
                var payTable = TryGetExistingTableName(connection, "PAY");
                if (!string.IsNullOrWhiteSpace(payTable))
                {
                    var payEnrollmentColumn = TryGetExistingColumnName(connection, payTable, "ID_enrolment");
                    var payStatusColumn = TryGetExistingColumnName(connection, payTable, "status", "State");
                    if (payEnrollmentColumn is not null && payStatusColumn is not null)
                    {
                        await using var updPay = connection.CreateCommand();
                        updPay.Transaction = tx;
                        updPay.CommandText = $"""
                            UPDATE [{payTable}]
                            SET [{payStatusColumn}] = ?
                            WHERE [{payEnrollmentColumn}] = ?
                            """;
                        updPay.Parameters.AddWithValue("@p1", "refunded");
                        updPay.Parameters.AddWithValue("@p2", enrollmentId);
                        _ = await updPay.ExecuteNonQueryAsync(cancellationToken);
                    }
                }
            }
            catch (OleDbException)
            {
                // PAY может быть связан ограничениями; отмену и возврат по балансу всё равно завершаем.
            }

            var userTable = GetExistingTableName(connection, "USER", "USERS");
            var userIdColumn = GetExistingColumnName(connection, userTable, "ID_user", "user_id");
            var userBalanceColumn = TryGetExistingColumnName(connection, userTable, "balanse_coins", "balance_coins", "coins_balance");
            var userRubBalanceColumn = TryGetUserRubBalanceColumn(connection, userTable);

            if (refund > 0)
            {
                if (userRubBalanceColumn is null)
                {
                    throw new InvalidOperationException($"USER: не удалось найти/создать колонку рублёвого баланса (ожидалась balance_rub/refund_balance_rub/rub_balance).");
                }

                await using var readRub = connection.CreateCommand();
                readRub.Transaction = tx;
                readRub.CommandText = $"""
                    SELECT [{userRubBalanceColumn}] AS rub_balance
                    FROM [{userTable}]
                    WHERE [{userIdColumn}] = ?
                    """;
                readRub.Parameters.AddWithValue("@p1", userId);
                var currentRubObj = await readRub.ExecuteScalarAsync(cancellationToken);
                var currentRub = ToDecimal(currentRubObj);
                var nextRub = currentRub + refund;

                await using var updateRubBalance = connection.CreateCommand();
                updateRubBalance.Transaction = tx;
                updateRubBalance.CommandText = $"""
                    UPDATE [{userTable}]
                    SET [{userRubBalanceColumn}] = ?
                    WHERE [{userIdColumn}] = ?
                    """;
                var pRub = updateRubBalance.Parameters.Add("@p1", OleDbType.Double);
                pRub.Value = Convert.ToDouble(nextRub);
                updateRubBalance.Parameters.AddWithValue("@p2", userId);
                _ = await updateRubBalance.ExecuteNonQueryAsync(cancellationToken);
            }

            // Бонусы (balanse_coins) были начислены как 3% от оплаты — списываем обратно пропорционально возврату.
            if (userBalanceColumn is not null && refund > 0)
            {
                var coinsToRollback = (int)Math.Floor(refund * 0.03m);
                if (coinsToRollback > 0)
                {
                    await using var readCoins = connection.CreateCommand();
                    readCoins.Transaction = tx;
                    readCoins.CommandText = $"""
                        SELECT [{userBalanceColumn}] AS coins_balance
                        FROM [{userTable}]
                        WHERE [{userIdColumn}] = ?
                        """;
                    readCoins.Parameters.AddWithValue("@p1", userId);
                    var currentCoinsObj = await readCoins.ExecuteScalarAsync(cancellationToken);
                    var currentCoins = ToInt(currentCoinsObj);
                    var nextCoins = Math.Max(0, currentCoins - coinsToRollback);

                    await using var rollbackCoins = connection.CreateCommand();
                    rollbackCoins.Transaction = tx;
                    rollbackCoins.CommandText = $"""
                        UPDATE [{userTable}]
                        SET [{userBalanceColumn}] = ?
                        WHERE [{userIdColumn}] = ?
                        """;
                    rollbackCoins.Parameters.AddWithValue("@p1", nextCoins);
                    rollbackCoins.Parameters.AddWithValue("@p2", userId);
                    _ = await rollbackCoins.ExecuteNonQueryAsync(cancellationToken);
                }
            }

            // Фиксируем факт проведённого возврата (защита от двойного начисления).
            if (refundProcessedColumn is not null || refundAmountColumn is not null)
            {
                var sets = new List<string>();
                var vals = new List<object?>();
                if (refundProcessedColumn is not null)
                {
                    sets.Add($"[{refundProcessedColumn}] = ?");
                    vals.Add(true);
                }
                if (refundAmountColumn is not null)
                {
                    sets.Add($"[{refundAmountColumn}] = ?");
                    vals.Add(Convert.ToDouble(refund));
                }

                if (sets.Count > 0)
                {
                    await using var mark = connection.CreateCommand();
                    mark.Transaction = tx;
                    mark.CommandText = $"""
                        UPDATE [{enrollmentTable}]
                        SET {string.Join(", ", sets)}
                        WHERE [{enrollmentIdColumn}] = ? AND [{enrollmentUserColumn}] = ? AND [{enrollmentCourseColumn}] = ?
                        """;
                    for (var i = 0; i < vals.Count; i++)
                    {
                        mark.Parameters.AddWithValue($"@p{i + 1}", vals[i] ?? DBNull.Value);
                    }
                    mark.Parameters.AddWithValue($"@p{vals.Count + 1}", selectedEnrollmentId);
                    mark.Parameters.AddWithValue($"@p{vals.Count + 2}", userId);
                    mark.Parameters.AddWithValue($"@p{vals.Count + 3}", courseId);
                    var markAffected = await mark.ExecuteNonQueryAsync(cancellationToken);
                    // В грязных данных могут быть дубли даже по (id+user+course). Это нормально:
                    // помечаем все такие строки как обработанные, но деньги возвращаем один раз (см. защиту выше).
                    if (markAffected <= 0)
                    {
                        throw new InvalidOperationException($"ENROLLMENTS: не удалось пометить refund_processed/refund_amount по заявке #{selectedEnrollmentId} (обновлено: {markAffected}).");
                    }
                }
            }

            tx.Commit();
            return true;
        }
        catch
        {
            tx.Rollback();
            throw;
        }
    }

    public async Task<bool> RejectRefundForEnrollmentAsync(int enrollmentId, int userId, int courseId, CancellationToken cancellationToken = default)
    {
        if (enrollmentId <= 0 || userId <= 0 || courseId <= 0)
        {
            return false;
        }

        await EnsureEnrollmentStatusColumnAsync(cancellationToken);

        await using var connection = new OleDbConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        var enrollmentTable = GetExistingTableName(connection, "ENROLLMENTS");
        var enrollmentIdColumn = GetExistingColumnName(connection, enrollmentTable, "ID_enrolment", "ID_enrollment", "enrollment_id");
        var enrollmentCourseColumn = GetEnrollmentCourseColumn(connection, enrollmentTable);
        var enrollmentUserColumn = GetExistingColumnName(connection, enrollmentTable, "ID_user", "user_id", "ID_users");
        var statusColumns = GetEnrollmentStatusColumns(connection, enrollmentTable);
        if (statusColumns.Count == 0)
        {
            return false;
        }

        // Читаем строку целиком и обновляем только её (id+user+course), иначе можно "не попасть" в нужную запись.
        await using var read = connection.CreateCommand();
        var selectCols = string.Join(", ", statusColumns.Select(c => $"[{c}]"));
        read.CommandText = $"""
            SELECT TOP 1 [{enrollmentUserColumn}] AS e_user,
                          [{enrollmentCourseColumn}] AS e_course,
                          {selectCols}
            FROM [{enrollmentTable}]
            WHERE [{enrollmentIdColumn}] = ? AND [{enrollmentUserColumn}] = ? AND [{enrollmentCourseColumn}] = ?
            """;
        read.Parameters.AddWithValue("@p1", enrollmentId);
        read.Parameters.AddWithValue("@p2", userId);
        read.Parameters.AddWithValue("@p3", courseId);
        await using var rr = await read.ExecuteReaderAsync(cancellationToken);
        if (rr is null || !await rr.ReadAsync(cancellationToken))
        {
            return false;
        }

        var current = string.Empty;
        foreach (var c in statusColumns)
        {
            var v = ToStringSafe(rr[c]).Trim();
            if (!string.IsNullOrWhiteSpace(v))
            {
                current = v;
                break;
            }
        }
        if (!string.Equals(current, "refund_requested", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        await using var cmd = connection.CreateCommand();
        var sets = string.Join(", ", statusColumns.Select(c => $"[{c}] = ?"));
        cmd.CommandText = $"""
            UPDATE [{enrollmentTable}]
            SET {sets}
            WHERE [{enrollmentIdColumn}] = ? AND [{enrollmentUserColumn}] = ? AND [{enrollmentCourseColumn}] = ?
            """;
        var p = 1;
        foreach (var _ in statusColumns)
        {
            cmd.Parameters.AddWithValue($"@p{p++}", "completed");
        }
        cmd.Parameters.AddWithValue($"@p{p++}", enrollmentId);
        cmd.Parameters.AddWithValue($"@p{p++}", userId);
        cmd.Parameters.AddWithValue($"@p{p}", courseId);
        var affected = await cmd.ExecuteNonQueryAsync(cancellationToken);
        if (affected != 1)
        {
            return false;
        }

        await using var verify = connection.CreateCommand();
        verify.CommandText = $"""
            SELECT TOP 1 {selectCols}
            FROM [{enrollmentTable}]
            WHERE [{enrollmentIdColumn}] = ? AND [{enrollmentUserColumn}] = ? AND [{enrollmentCourseColumn}] = ?
            """;
        verify.Parameters.AddWithValue("@p1", enrollmentId);
        verify.Parameters.AddWithValue("@p2", userId);
        verify.Parameters.AddWithValue("@p3", courseId);
        await using var vr = await verify.ExecuteReaderAsync(cancellationToken);
        if (vr is null || !await vr.ReadAsync(cancellationToken))
        {
            return false;
        }
        foreach (var c in statusColumns)
        {
            var v = ToStringSafe(vr[c]).Trim();
            if (!string.IsNullOrWhiteSpace(v))
            {
                return string.Equals(v, "completed", StringComparison.OrdinalIgnoreCase);
            }
        }
        return false;
    }

    public async Task<IReadOnlyList<EnrollmentItem>> GetRefundRequestedEnrollmentsAsync(CancellationToken cancellationToken = default)
    {
        var result = new List<EnrollmentItem>();
        await using var connection = new OleDbConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        var enrollmentTable = GetExistingTableName(connection, "ENROLLMENTS");
        var enrollmentIdColumn = GetExistingColumnName(connection, enrollmentTable, "ID_enrolment", "ID_enrollment", "enrollment_id");
        var enrollmentCourseColumn = GetEnrollmentCourseColumn(connection, enrollmentTable);
        var enrollmentUserColumn = GetExistingColumnName(connection, enrollmentTable, "ID_user", "user_id", "ID_users");
        var enrollmentPayMethodColumn = TryGetExistingColumnName(connection, enrollmentTable, "PAY_method");
        var enrollmentPriceColumn = TryGetExistingColumnName(connection, enrollmentTable, "price");

        var statusColumns = GetEnrollmentStatusColumns(connection, enrollmentTable);
        if (statusColumns.Count == 0)
        {
            return result;
        }

        var payMethodSelect = enrollmentPayMethodColumn is null ? "''" : $"[{enrollmentPayMethodColumn}]";
        var priceSelect = enrollmentPriceColumn is null ? "0" : $"[{enrollmentPriceColumn}]";
        var statusProjection = string.Join(", ", statusColumns.Select((c, i) => $"[{c}] AS s{i}"));
        var where = string.Join(" OR ", statusColumns.Select(c => $"LCase(Trim([{c}])) = 'refund_requested'"));

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $"""
            SELECT
                [{enrollmentIdColumn}] AS eid,
                [{enrollmentCourseColumn}] AS cid,
                [{enrollmentUserColumn}] AS uid,
                {payMethodSelect} AS pm,
                {statusProjection},
                {priceSelect} AS pr
            FROM [{enrollmentTable}]
            WHERE ({where})
            ORDER BY [{enrollmentIdColumn}] DESC
            """;

        await using var r = await cmd.ExecuteReaderAsync(cancellationToken);
        if (r is null) return result;

        while (await r.ReadAsync(cancellationToken))
        {
            var st = string.Empty;
            for (var i = 0; i < statusColumns.Count; i++)
            {
                var v = ToStringSafe(r[$"s{i}"]).Trim();
                if (!string.IsNullOrWhiteSpace(v))
                {
                    st = v;
                    break;
                }
            }

            result.Add(new EnrollmentItem
            {
                ID_enrolment = ToInt(r["eid"]),
                ID_cours = ToInt(r["cid"]),
                ID_user = ToInt(r["uid"]),
                PAY_method = ToStringSafe(r["pm"]),
                status = st,
                price = ToDecimal(r["pr"])
            });
        }

        return result;
    }

    public async Task<int> ClearRefundRequestsAsync(bool onlyProcessed, CancellationToken cancellationToken = default)
    {
        await EnsureEnrollmentStatusColumnAsync(cancellationToken);
        await EnsureEnrollmentRefundColumnsAsync(cancellationToken);

        await using var connection = new OleDbConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        var enrollmentTable = GetExistingTableName(connection, "ENROLLMENTS");
        var enrollmentIdColumn = GetExistingColumnName(connection, enrollmentTable, "ID_enrolment", "ID_enrollment", "enrollment_id");
        var enrollmentCourseColumn = GetEnrollmentCourseColumn(connection, enrollmentTable);
        var enrollmentUserColumn = GetExistingColumnName(connection, enrollmentTable, "ID_user", "user_id", "ID_users");

        var statusColumns = GetEnrollmentStatusColumns(connection, enrollmentTable);
        if (statusColumns.Count == 0)
        {
            return 0;
        }

        var processedCol = TryGetEnrollmentRefundProcessedColumn(connection, enrollmentTable);
        var amountCol = TryGetEnrollmentRefundAmountColumn(connection, enrollmentTable);

        // UPDATE sets: cancel in all status columns
        var setStatuses = string.Join(", ", statusColumns.Select(c => $"[{c}] = ?"));

        // WHERE: any status column equals refund_requested (with Trim+LCase)
        var whereStatus = string.Join(" OR ", statusColumns.Select(c => $"LCase(Trim([{c}])) = 'refund_requested'"));
        var where = $"({whereStatus})";

        if (onlyProcessed)
        {
            var processedParts = new List<string>();
            if (!string.IsNullOrWhiteSpace(processedCol))
            {
                processedParts.Add($"[{processedCol}] = True");
            }
            if (!string.IsNullOrWhiteSpace(amountCol))
            {
                processedParts.Add($"[{amountCol}] > 0");
            }
            if (processedParts.Count > 0)
            {
                where += $" AND ({string.Join(" OR ", processedParts)})";
            }
        }

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $"""
            UPDATE [{enrollmentTable}]
            SET {setStatuses}
            WHERE {where}
            """;

        // parameters for SET (one per status col)
        var p = 1;
        foreach (var _ in statusColumns)
        {
            cmd.Parameters.AddWithValue($"@p{p++}", "cancelled");
        }

        var affected = await cmd.ExecuteNonQueryAsync(cancellationToken);
        return affected;
    }

    public async Task UpdateUserCourseAsync(int userId, int courseId, CancellationToken cancellationToken = default)
    {
        await using var connection = new OleDbConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        await using var command = connection.CreateCommand();
        command.CommandText = """
            UPDATE [USER]
            SET ID_curs = ?
            WHERE ID_user = ?
            """;
        command.Parameters.AddWithValue("@p1", courseId);
        command.Parameters.AddWithValue("@p2", userId);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task UpdateUserProfileAsync(
        int userId,
        string fullName,
        string email,
        string phone,
        CancellationToken cancellationToken = default)
    {
        await using var connection = new OleDbConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        await using var command = connection.CreateCommand();
        command.CommandText = """
            UPDATE [USER]
            SET full_name = ?, email = ?, phone = ?
            WHERE ID_user = ?
            """;
        command.Parameters.AddWithValue("@p1", ToStringSafe(fullName));
        command.Parameters.AddWithValue("@p2", NormalizeEmail(email));
        command.Parameters.AddWithValue("@p3", ToStringSafe(phone));
        command.Parameters.AddWithValue("@p4", userId);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<bool> UpdateUserAdminAsync(
        int userId,
        string fullName,
        string email,
        string phone,
        string role,
        int? balanseCoins,
        decimal? balanceRub,
        int rewardCoins,
        int? currentCourseId,
        int? homeworkId,
        CancellationToken cancellationToken = default)
    {
        await using var connection = new OleDbConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        await using var command = connection.CreateCommand();
        var userTable = GetExistingTableName(connection, "USER", "USERS");
        var rubCol = TryGetUserRubBalanceColumn(connection, userTable);
        command.CommandText = rubCol is null ? """
            UPDATE [USER]
            SET full_name = ?,
                email = ?,
                phone = ?,
                role = ?,
                balanse_coins = ?,
                reward_coins = ?,
                ID_curs = ?,
                ID__homework = ?
            WHERE ID_user = ?
            """ : $"""
            UPDATE [{userTable}]
            SET full_name = ?,
                email = ?,
                phone = ?,
                role = ?,
                balanse_coins = ?,
                [{rubCol}] = ?,
                reward_coins = ?,
                ID_curs = ?,
                ID__homework = ?
            WHERE ID_user = ?
            """;
        command.Parameters.AddWithValue("@p1", ToStringSafe(fullName));
        command.Parameters.AddWithValue("@p2", NormalizeEmail(email));
        command.Parameters.AddWithValue("@p3", ToStringSafe(phone));
        command.Parameters.AddWithValue("@p4", ToStringSafe(role));
        command.Parameters.AddWithValue("@p5", balanseCoins.HasValue ? balanseCoins.Value : (object)DBNull.Value);
        var shift = 0;
        if (rubCol is not null)
        {
            command.Parameters.AddWithValue("@p6", balanceRub.HasValue ? balanceRub.Value : (object)DBNull.Value);
            shift = 1;
        }
        command.Parameters.AddWithValue($"@p{6 + shift}", rewardCoins);
        command.Parameters.AddWithValue($"@p{7 + shift}", currentCourseId.HasValue ? currentCourseId.Value : (object)DBNull.Value);
        command.Parameters.AddWithValue($"@p{8 + shift}", homeworkId.HasValue ? homeworkId.Value : (object)DBNull.Value);
        command.Parameters.AddWithValue($"@p{9 + shift}", userId);

        return await command.ExecuteNonQueryAsync(cancellationToken) > 0;
    }

    public async Task EnsureUserModerationColumnsAsync(CancellationToken cancellationToken = default)
    {
        if (_userModerationSchemaEnsured)
        {
            return;
        }

        // Миграции схемы не должны падать из-за отмены HTTP-запроса.
        await _schemaLock.WaitAsync(CancellationToken.None);
        try
        {
            if (_userModerationSchemaEnsured)
            {
                return;
            }

            // Важно: ALTER TABLE в Access может блокировать таблицу.
            // Делаем эту операцию один раз на процесс, чтобы не ловить блокировки при обычных запросах.
            await using var connection = new OleDbConnection(_connectionString);
            await connection.OpenAsync(CancellationToken.None);

            await TryAddColumnAsync(connection, "USER", "is_blocked", "YESNO", CancellationToken.None);
            await TryAddColumnAsync(connection, "USER", "blocked_until", "DATETIME", CancellationToken.None);
            // В Access текстовое поле большой длины — MEMO (LONGTEXT не везде проходит).
            await TryAddColumnAsync(connection, "USER", "block_reason", "MEMO", CancellationToken.None);
            await TryAddColumnAsync(connection, "USER", "is_deleted", "YESNO", CancellationToken.None);

            _userModerationSchemaEnsured = true;
        }
        finally
        {
            _schemaLock.Release();
        }
    }

    public async Task<bool> SetUserBlockedAsync(int userId, bool isBlocked, DateTime? blockedUntil, string? reason, CancellationToken cancellationToken = default)
    {
        await EnsureUserModerationColumnsAsync(cancellationToken);

        await using var connection = new OleDbConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var blockedCol = TryGetExistingColumnName(connection, "USER", "is_blocked");
        var untilCol = TryGetExistingColumnName(connection, "USER", "blocked_until");
        var reasonCol = TryGetExistingColumnName(connection, "USER", "block_reason");

        if (blockedCol is null)
        {
            return false;
        }

        var sets = new List<string> { $"[{blockedCol}] = ?" };
        if (untilCol is not null) sets.Add($"[{untilCol}] = ?");
        if (reasonCol is not null) sets.Add($"[{reasonCol}] = ?");

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $"""
            UPDATE [USER]
            SET {string.Join(", ", sets)}
            WHERE ID_user = ?
            """;
        cmd.Parameters.AddWithValue("@p1", isBlocked);
        var p = 2;
        if (untilCol is not null)
        {
            cmd.Parameters.AddWithValue($"@p{p++}", blockedUntil.HasValue ? blockedUntil.Value : (object)DBNull.Value);
        }
        if (reasonCol is not null)
        {
            cmd.Parameters.AddWithValue($"@p{p++}", string.IsNullOrWhiteSpace(reason) ? (object)DBNull.Value : ToStringSafe(reason));
        }
        cmd.Parameters.AddWithValue($"@p{p}", userId);
        return await cmd.ExecuteNonQueryAsync(cancellationToken) > 0;
    }

    public async Task<bool> SetUserDeletedAsync(int userId, bool isDeleted, CancellationToken cancellationToken = default)
    {
        await EnsureUserModerationColumnsAsync(cancellationToken);

        await using var connection = new OleDbConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var deletedCol = GetExistingColumnName(connection, "USER", "is_deleted");
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $"""
            UPDATE [USER]
            SET [{deletedCol}] = ?
            WHERE ID_user = ?
            """;
        cmd.Parameters.AddWithValue("@p1", isDeleted);
        cmd.Parameters.AddWithValue("@p2", userId);
        return await cmd.ExecuteNonQueryAsync(cancellationToken) > 0;
    }

    private static async Task TryAddColumnAsync(
        OleDbConnection connection,
        string tableName,
        string columnName,
        string accessType,
        CancellationToken cancellationToken)
    {
        // если колонка уже есть — ничего не делаем
        if (TryGetExistingColumnName(connection, tableName, columnName) is not null)
        {
            return;
        }

        try
        {
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = $"ALTER TABLE [{tableName}] ADD COLUMN [{columnName}] {accessType}";
            await cmd.ExecuteNonQueryAsync(cancellationToken);
        }
        catch (OleDbException)
        {
            // Если нет прав/тип не поддержан — молча игнорируем (функции блокировки тогда работать не будут)
        }
    }

    public async Task<bool> ChangeUserPasswordAsync(
        int userId,
        string currentPassword,
        string newPassword,
        string? currentEmail = null,
        CancellationToken cancellationToken = default)
    {
        await using var connection = new OleDbConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        var normalizedEmail = NormalizeEmail(currentEmail ?? string.Empty);

        // 1) Verify current password strictly in DB by ID.
        await using var verifyById = connection.CreateCommand();
        verifyById.CommandText = "SELECT COUNT(*) FROM [USER] WHERE ID_user = ? AND [password] = ?";
        verifyById.Parameters.AddWithValue("@p1", userId);
        verifyById.Parameters.AddWithValue("@p2", currentPassword);
        var matchesById = ToInt(await verifyById.ExecuteScalarAsync(cancellationToken));

        var targetUserId = userId;
        if (matchesById <= 0 && !string.IsNullOrWhiteSpace(normalizedEmail))
        {
            // 2) Fallback verify by email if session ID mismatched.
            await using var verifyByEmail = connection.CreateCommand();
            verifyByEmail.CommandText = """
                SELECT TOP 1 ID_user
                FROM [USER]
                WHERE (email = ? OR email = ?) AND [password] = ?
                """;
            verifyByEmail.Parameters.AddWithValue("@p1", normalizedEmail);
            verifyByEmail.Parameters.AddWithValue("@p2", $"\"{normalizedEmail}\"");
            verifyByEmail.Parameters.AddWithValue("@p3", currentPassword);
            var matchedUserId = ToInt(await verifyByEmail.ExecuteScalarAsync(cancellationToken));
            if (matchedUserId > 0)
            {
                targetUserId = matchedUserId;
            }
            else
            {
                return false;
            }
        }
        else if (matchesById <= 0)
        {
            return false;
        }

        // 3) Update exact matched row.
        await using var updateCommand = connection.CreateCommand();
        updateCommand.CommandText = "UPDATE [USER] SET [password] = ? WHERE ID_user = ?";
        updateCommand.Parameters.AddWithValue("@p1", newPassword);
        updateCommand.Parameters.AddWithValue("@p2", targetUserId);
        var affectedRows = await updateCommand.ExecuteNonQueryAsync(cancellationToken);
        if (affectedRows <= 0)
        {
            return false;
        }

        // 4) Verify persisted value from DB.
        await using var verifySaved = connection.CreateCommand();
        verifySaved.CommandText = "SELECT [password] FROM [USER] WHERE ID_user = ?";
        verifySaved.Parameters.AddWithValue("@p1", targetUserId);
        var savedPassword = (await verifySaved.ExecuteScalarAsync(cancellationToken))?.ToString() ?? string.Empty;
        return string.Equals(savedPassword, newPassword, StringComparison.Ordinal);
    }

    public async Task<IReadOnlyList<PaymentItem>> GetPaymentsByUserIdAsync(int userId, CancellationToken cancellationToken = default)
    {
        var result = new List<PaymentItem>();
        await using var connection = new OleDbConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        var payTable = GetExistingTableName(connection, "PAY");
        var payUserColumn = GetExistingColumnName(connection, payTable, "ID_user");
        var payCourseColumn = GetExistingColumnName(connection, payTable, "ID_cours", "ID_curs");
        var payEnrollmentColumn = GetExistingColumnName(connection, payTable, "ID_enrolment");
        var payHomeworkColumn = TryGetExistingColumnName(connection, payTable, "ID_homework");
        var payMethodColumn = TryGetExistingColumnName(connection, payTable, "PAY_method");
        var payStatusColumn = TryGetExistingColumnName(connection, payTable, "status", "State");
        var payPriceColumn = TryGetExistingColumnName(connection, payTable, "price", "Price");
        var payHomeworkSelect = payHomeworkColumn is null ? "0" : $"[{payHomeworkColumn}]";
        var payMethodSelect = payMethodColumn is null ? "''" : $"[{payMethodColumn}]";
        var payStatusSelect = payStatusColumn is null ? "''" : $"[{payStatusColumn}]";
        var payPriceSelect = payPriceColumn is null ? "0" : $"[{payPriceColumn}]";

        await using var command = connection.CreateCommand();
        command.CommandText = $"""
            SELECT [{payUserColumn}] AS pay_user_id,
                   {payHomeworkSelect} AS pay_homework_id,
                   [{payCourseColumn}] AS pay_course_id,
                   [{payEnrollmentColumn}] AS pay_enrollment_id,
                   {payMethodSelect} AS pay_method,
                   {payStatusSelect} AS pay_status,
                   {payPriceSelect} AS pay_price
            FROM [{payTable}]
            WHERE [{payUserColumn}] = ?
            ORDER BY [{payEnrollmentColumn}] DESC
            """;
        command.Parameters.AddWithValue("@p1", userId);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (reader is null)
        {
            return result;
        }

        while (await reader.ReadAsync(cancellationToken))
        {
            result.Add(new PaymentItem
            {
                ID_user = ToInt(reader["pay_user_id"]),
                ID_homework = ToInt(reader["pay_homework_id"]),
                ID_curs = ToInt(reader["pay_course_id"]),
                ID_enrolment = ToInt(reader["pay_enrollment_id"]),
                PAY_method = ToStringSafe(reader["pay_method"]),
                status = ToStringSafe(reader["pay_status"]),
                price = ToDecimal(reader["pay_price"])
            });
        }

        return result;
    }

    public async Task<IReadOnlyList<PaymentItem>> GetAllPaymentsAsync(CancellationToken cancellationToken = default)
    {
        var result = new List<PaymentItem>();
        await using var connection = new OleDbConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        var payTable = GetExistingTableName(connection, "PAY");
        var payUserColumn = GetExistingColumnName(connection, payTable, "ID_user");
        var payCourseColumn = GetExistingColumnName(connection, payTable, "ID_cours", "ID_curs");
        var payEnrollmentColumn = GetExistingColumnName(connection, payTable, "ID_enrolment");
        var payHomeworkColumn = TryGetExistingColumnName(connection, payTable, "ID_homework");
        var payMethodColumn = TryGetExistingColumnName(connection, payTable, "PAY_method");
        var payStatusColumn = TryGetExistingColumnName(connection, payTable, "status", "State");
        var payPriceColumn = TryGetExistingColumnName(connection, payTable, "price", "Price");
        var payHomeworkSelect = payHomeworkColumn is null ? "0" : $"[{payHomeworkColumn}]";
        var payMethodSelect = payMethodColumn is null ? "''" : $"[{payMethodColumn}]";
        var payStatusSelect = payStatusColumn is null ? "''" : $"[{payStatusColumn}]";
        var payPriceSelect = payPriceColumn is null ? "0" : $"[{payPriceColumn}]";

        await using var command = connection.CreateCommand();
        command.CommandText = $"""
            SELECT [{payUserColumn}] AS pay_user_id,
                   {payHomeworkSelect} AS pay_homework_id,
                   [{payCourseColumn}] AS pay_course_id,
                   [{payEnrollmentColumn}] AS pay_enrollment_id,
                   {payMethodSelect} AS pay_method,
                   {payStatusSelect} AS pay_status,
                   {payPriceSelect} AS pay_price
            FROM [{payTable}]
            ORDER BY [{payEnrollmentColumn}] DESC
            """;

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (reader is null)
        {
            return result;
        }

        while (await reader.ReadAsync(cancellationToken))
        {
            result.Add(new PaymentItem
            {
                ID_user = ToInt(reader["pay_user_id"]),
                ID_homework = ToInt(reader["pay_homework_id"]),
                ID_curs = ToInt(reader["pay_course_id"]),
                ID_enrolment = ToInt(reader["pay_enrollment_id"]),
                PAY_method = ToStringSafe(reader["pay_method"]),
                status = ToStringSafe(reader["pay_status"]),
                price = ToDecimal(reader["pay_price"])
            });
        }

        return result;
    }

    public async Task<IReadOnlyList<EnrollmentItem>> GetAllEnrollmentsAsync(CancellationToken cancellationToken = default)
    {
        var result = new List<EnrollmentItem>();
        await using var connection = new OleDbConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        var enrollmentTable = GetExistingTableName(connection, "ENROLLMENTS");
        var enrollmentIdColumn = GetExistingColumnName(connection, enrollmentTable, "ID_enrolment", "ID_enrollment", "enrollment_id");
        var enrollmentCourseColumn = GetEnrollmentCourseColumn(connection, enrollmentTable);
        var enrollmentUserColumn = GetExistingColumnName(connection, enrollmentTable, "ID_user", "user_id", "ID_users");
        var enrollmentPayMethodColumn = TryGetExistingColumnName(connection, enrollmentTable, "PAY_method");
        var statusColumns = GetEnrollmentStatusColumns(connection, enrollmentTable);
        var enrollmentPriceColumn = TryGetExistingColumnName(connection, enrollmentTable, "price");

        var payMethodSelect = enrollmentPayMethodColumn is null ? "''" : $"[{enrollmentPayMethodColumn}]";
        var statusSelect = statusColumns.Count == 0 ? "''" : string.Join(", ", statusColumns.Select(c => $"[{c}]"));
        var priceSelect = enrollmentPriceColumn is null ? "0" : $"[{enrollmentPriceColumn}]";

        await using var command = connection.CreateCommand();
        var statusProjection = statusColumns.Count == 0
            ? "'' AS enrollment_status"
            : string.Join(", ", statusColumns.Select((c, i) => $"[{c}] AS enrollment_status_{i}"));
        command.CommandText = $"""
            SELECT
                [{enrollmentIdColumn}] AS enrollment_id,
                [{enrollmentCourseColumn}] AS enrollment_course_id,
                [{enrollmentUserColumn}] AS enrollment_user_id,
                {payMethodSelect} AS enrollment_pay_method,
                {statusProjection},
                {priceSelect} AS enrollment_price
            FROM [{enrollmentTable}]
            ORDER BY [{enrollmentIdColumn}] DESC
            """;

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (reader is null)
        {
            return result;
        }

        while (await reader.ReadAsync(cancellationToken))
        {
            result.Add(new EnrollmentItem
            {
                ID_enrolment = ToInt(reader["enrollment_id"]),
                ID_cours = ToInt(reader["enrollment_course_id"]),
                ID_user = ToInt(reader["enrollment_user_id"]),
                PAY_method = ToStringSafe(reader["enrollment_pay_method"]),
                status = statusColumns.Count == 0
                    ? string.Empty
                    : statusColumns
                        .Select((_, i) => ToStringSafe(reader[$"enrollment_status_{i}"]).Trim())
                        .FirstOrDefault(s => !string.IsNullOrWhiteSpace(s)) ?? string.Empty,
                price = ToDecimal(reader["enrollment_price"])
            });
        }

        return result;
    }

    public async Task<IReadOnlyList<HomeworkItem>> GetHomeworkByUserIdAsync(int userId, CancellationToken cancellationToken = default)
    {
        var result = new List<HomeworkItem>();
        var payments = await GetPaymentsByUserIdAsync(userId, cancellationToken);
        var homeworkIds = payments
            .Where(p => p.ID_homework > 0)
            .Select(p => p.ID_homework)
            .Distinct()
            .ToArray();

        await using var connection = new OleDbConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        var homeworkTable = GetExistingTableName(connection, "HOMEWORK", "PROGRES");

        if (homeworkIds.Length == 0)
        {
            await using var fallbackCommand = connection.CreateCommand();
            fallbackCommand.CommandText = $"""
                SELECT ID_lesson, ID_homework, Progres_bal, status, grade
                FROM [{homeworkTable}]
                ORDER BY ID_homework
                """;

            await using var fallbackReader = await fallbackCommand.ExecuteReaderAsync(cancellationToken);
            if (fallbackReader is null)
            {
                return result;
            }

            while (await fallbackReader.ReadAsync(cancellationToken))
            {
                result.Add(new HomeworkItem
                {
                    ID_lesson = ToInt(fallbackReader["ID_lesson"]),
                    ID_homework = ToInt(fallbackReader["ID_homework"]),
                    Progres_bal = ToInt(fallbackReader["Progres_bal"]),
                    status = ToStringSafe(fallbackReader["status"]),
                    grade = ToNullableInt(fallbackReader["grade"])
                });
            }

            return result;
        }

        foreach (var homeworkId in homeworkIds)
        {
            await using var command = connection.CreateCommand();
            command.CommandText = $"""
                SELECT ID_lesson, ID_homework, Progres_bal, status, grade
                FROM [{homeworkTable}]
                WHERE ID_homework = ?
                """;
            command.Parameters.AddWithValue("@p1", homeworkId);

            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (reader is null)
            {
                continue;
            }

            while (await reader.ReadAsync(cancellationToken))
            {
                result.Add(new HomeworkItem
                {
                    ID_lesson = ToInt(reader["ID_lesson"]),
                    ID_homework = ToInt(reader["ID_homework"]),
                    Progres_bal = ToInt(reader["Progres_bal"]),
                    status = ToStringSafe(reader["status"]),
                    grade = ToNullableInt(reader["grade"])
                });
            }
        }

        return result;
    }

    public async Task<IReadOnlyList<AdItem>> GetAdsAsync(CancellationToken cancellationToken = default)
    {
        var result = new List<AdItem>();
        await using var connection = new OleDbConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        var adTable = GetExistingTableName(connection, "AD");

        await using var command = connection.CreateCommand();
        command.CommandText = $"SELECT ID_AD, AD_type, reward_coins FROM [{adTable}] ORDER BY ID_AD";

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (reader is null)
        {
            return result;
        }

        while (await reader.ReadAsync(cancellationToken))
        {
            result.Add(new AdItem
            {
                ID_AD = ToInt(reader["ID_AD"]),
                AD_type = ToStringSafe(reader["AD_type"]),
                reward_coins = ToInt(reader["reward_coins"])
            });
        }

        return result;
    }

    public async Task<IReadOnlyList<ShowAdItem>> GetAdViewsByUserIdAsync(int userId, CancellationToken cancellationToken = default)
    {
        var result = new List<ShowAdItem>();
        await using var connection = new OleDbConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        var showAdTable = GetExistingTableName(connection, "SHOW_AD");

        await using var command = connection.CreateCommand();
        command.CommandText = $"""
            SELECT ID_AD, ID_user, reward_coins
            FROM [{showAdTable}]
            WHERE ID_user = ?
            ORDER BY ID_AD
            """;
        command.Parameters.AddWithValue("@p1", userId);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (reader is null)
        {
            return result;
        }

        while (await reader.ReadAsync(cancellationToken))
        {
            result.Add(new ShowAdItem
            {
                ID_AD = ToInt(reader["ID_AD"]),
                ID_user = ToInt(reader["ID_user"]),
                reward_coins = ToInt(reader["reward_coins"])
            });
        }

        return result;
    }

    public async Task<IReadOnlyList<CourseReviewItem>> GetReviewsByCourseIdAsync(int courseId, CancellationToken cancellationToken = default)
    {
        var result = new List<CourseReviewItem>();

        await using var connection = new OleDbConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        var reviewsTable = TryGetExistingTableName(connection, "REVIEWS", "REVIEW");
        if (string.IsNullOrWhiteSpace(reviewsTable))
        {
            return result;
        }

        var reviewIdColumn = GetExistingColumnName(connection, reviewsTable, "ID_review");
        var reviewCourseColumn = GetExistingColumnName(connection, reviewsTable, "ID_cours", "ID_curs");
        var reviewUserColumn = GetExistingColumnName(connection, reviewsTable, "ID_user");
        var reviewRatingColumn = GetExistingColumnName(connection, reviewsTable, "rating");
        var reviewCommentColumn = GetExistingColumnName(connection, reviewsTable, "comment_text", "comment");
        var reviewCreatedAtColumn = GetExistingColumnName(connection, reviewsTable, "created_at", "create_at");

        await using var command = connection.CreateCommand();
        command.CommandText = $"""
            SELECT r.[{reviewIdColumn}] AS review_id,
                   r.[{reviewCourseColumn}] AS review_course_id,
                   r.[{reviewUserColumn}] AS review_user_id,
                   r.[{reviewRatingColumn}] AS review_rating,
                   r.[{reviewCommentColumn}] AS review_comment,
                   r.[{reviewCreatedAtColumn}] AS review_created_at,
                   u.[full_name] AS review_user_name
            FROM [{reviewsTable}] AS r
            LEFT JOIN [USER] AS u ON u.[ID_user] = r.[{reviewUserColumn}]
            WHERE r.[{reviewCourseColumn}] = ?
            ORDER BY r.[{reviewCreatedAtColumn}] DESC, r.[{reviewIdColumn}] DESC
            """;
        command.Parameters.AddWithValue("@p1", courseId);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (reader is null)
        {
            return result;
        }

        while (await reader.ReadAsync(cancellationToken))
        {
            result.Add(new CourseReviewItem
            {
                ID_review = ToInt(reader["review_id"]),
                ID_cours = ToInt(reader["review_course_id"]),
                ID_user = ToInt(reader["review_user_id"]),
                rating = ToDecimal(reader["review_rating"]),
                comment_text = ToStringSafe(reader["review_comment"]),
                created_at = reader["review_created_at"] is DBNull
                    ? DateTime.UtcNow
                    : Convert.ToDateTime(reader["review_created_at"]),
                user_name = ToStringSafe(reader["review_user_name"])
            });
        }

        return result;
    }

    /// <summary>
    /// Средний рейтинг и число отзывов по каждому курсу (таблица REVIEWS).
    /// </summary>
    public async Task<IReadOnlyDictionary<int, (decimal AverageRating, int ReviewCount)>> GetCourseRatingAggregatesAsync(
        CancellationToken cancellationToken = default)
    {
        var aggregates = new Dictionary<int, (decimal AverageRating, int ReviewCount)>();

        await using var connection = new OleDbConnection(_connectionString);
        await connection.OpenAsync(CancellationToken.None);
        var reviewsTable = TryGetExistingTableName(connection, "REVIEWS", "REVIEW");
        if (string.IsNullOrWhiteSpace(reviewsTable))
        {
            return aggregates;
        }

        var reviewCourseColumn = GetExistingColumnName(connection, reviewsTable, "ID_cours", "ID_curs");
        var reviewRatingColumn = GetExistingColumnName(connection, reviewsTable, "rating");

        await using var command = connection.CreateCommand();
        command.CommandText = $"SELECT [{reviewCourseColumn}], [{reviewRatingColumn}] FROM [{reviewsTable}]";
        await using var reader = await command.ExecuteReaderAsync(CancellationToken.None);
        if (reader is null)
        {
            return aggregates;
        }

        var groups = new Dictionary<int, List<decimal>>();
        while (await reader.ReadAsync(CancellationToken.None))
        {
            var courseId = ToInt(reader[reviewCourseColumn]);
            if (courseId <= 0)
            {
                continue;
            }

            var rating = ToDecimal(reader[reviewRatingColumn]);
            if (!groups.TryGetValue(courseId, out var list))
            {
                list = [];
                groups[courseId] = list;
            }

            list.Add(rating);
        }

        foreach (var kv in groups)
        {
            var list = kv.Value;
            if (list.Count == 0)
            {
                continue;
            }

            var sum = list.Aggregate(0m, (a, b) => a + b);
            var avg = Math.Round(sum / list.Count, 2, MidpointRounding.AwayFromZero);
            aggregates[kv.Key] = (avg, list.Count);
        }

        return aggregates;
    }

    private static IReadOnlyList<CourseItem> ApplyReviewAggregatesToCourses(
        IReadOnlyList<CourseItem> courses,
        IReadOnlyDictionary<int, (decimal AverageRating, int ReviewCount)> aggregates)
    {
        if (aggregates.Count == 0)
        {
            return courses;
        }

        var list = new List<CourseItem>(courses.Count);
        foreach (var c in courses)
        {
            if (aggregates.TryGetValue(c.ID_curs, out var s) && s.ReviewCount > 0)
            {
                list.Add(CourseItemWithRating(c, s.AverageRating, s.ReviewCount));
            }
            else
            {
                list.Add(c);
            }
        }

        return list;
    }

    private static CourseItem CourseItemWithRating(CourseItem c, decimal rating, int reviewCount) =>
        new()
        {
            ID_curs = c.ID_curs,
            ID_lesson = c.ID_lesson,
            ID_categorise = c.ID_categorise,
            price = c.price,
            Course_name = c.Course_name,
            rating = rating,
            create_at = c.create_at,
            preview_url = c.preview_url,
            ReviewCount = reviewCount
        };

    public async Task<bool> HasUserPaidCourseAsync(int userId, int courseId, CancellationToken cancellationToken = default)
    {
        var enrollment = await GetEnrollmentByUserAndCourseAsync(userId, courseId, cancellationToken);
        if (enrollment is not null)
        {
            return true;
        }

        var payments = await GetPaymentsByUserIdAsync(userId, cancellationToken);
        return payments.Any(p => p.ID_curs == courseId);
    }

    public async Task<bool> UpsertCourseReviewAsync(
        int userId,
        int courseId,
        decimal rating,
        string commentText,
        CancellationToken cancellationToken = default)
    {
        if (userId <= 0 || courseId <= 0)
        {
            return false;
        }

        await using var connection = new OleDbConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        var reviewsTable = TryGetExistingTableName(connection, "REVIEWS", "REVIEW");
        if (string.IsNullOrWhiteSpace(reviewsTable))
        {
            return false;
        }

        var reviewIdColumn = GetExistingColumnName(connection, reviewsTable, "ID_review");
        var reviewCourseColumn = GetExistingColumnName(connection, reviewsTable, "ID_cours", "ID_curs");
        var reviewUserColumn = GetExistingColumnName(connection, reviewsTable, "ID_user");
        var reviewRatingColumn = GetExistingColumnName(connection, reviewsTable, "rating");
        var reviewCommentColumn = GetExistingColumnName(connection, reviewsTable, "comment_text", "comment");
        var reviewCreatedAtColumn = GetExistingColumnName(connection, reviewsTable, "created_at", "create_at");

        static string ReviewWhereNum(string col) =>
            $"CLng(IIf(IsNumeric([{col}]), [{col}], 0))";

        var whereCourse = ReviewWhereNum(reviewCourseColumn);
        var whereUser = ReviewWhereNum(reviewUserColumn);
        var commentParam = commentText ?? string.Empty;

        async Task<int> UpdateByUserCoursePairAsync(bool useNumericWhere)
        {
            var where = useNumericWhere
                ? $"{whereCourse} = ? AND {whereUser} = ?"
                : $"[{reviewCourseColumn}] = ? AND [{reviewUserColumn}] = ?";

            await using var update = connection.CreateCommand();
            update.CommandText = $"""
                UPDATE [{reviewsTable}]
                SET [{reviewRatingColumn}] = ?,
                    [{reviewCommentColumn}] = ?,
                    [{reviewCreatedAtColumn}] = ?
                WHERE {where}
                """;
            update.Parameters.Add(new OleDbParameter("@r", OleDbType.Decimal) { Value = rating });
            update.Parameters.Add(new OleDbParameter("@txt", OleDbType.VarWChar)
            {
                Value = commentParam,
                Size = Math.Min(4000, Math.Max(1, commentParam.Length))
            });
            update.Parameters.Add(new OleDbParameter("@dt", OleDbType.Date) { Value = DateTime.Now });
            update.Parameters.Add(new OleDbParameter("@c", OleDbType.Integer) { Value = courseId });
            update.Parameters.Add(new OleDbParameter("@u", OleDbType.Integer) { Value = userId });
            return await update.ExecuteNonQueryAsync(cancellationToken);
        }

        async Task<int> CountPairAsync(bool useNumericWhere)
        {
            var where = useNumericWhere
                ? $"{whereCourse} = ? AND {whereUser} = ?"
                : $"[{reviewCourseColumn}] = ? AND [{reviewUserColumn}] = ?";

            await using var cmd = connection.CreateCommand();
            cmd.CommandText = $"SELECT COUNT(*) FROM [{reviewsTable}] WHERE {where}";
            cmd.Parameters.Add(new OleDbParameter("@c", OleDbType.Integer) { Value = courseId });
            cmd.Parameters.Add(new OleDbParameter("@u", OleDbType.Integer) { Value = userId });
            return ToInt(await cmd.ExecuteScalarAsync(cancellationToken));
        }

        async Task<bool> TryUpdateExistingReviewAsync()
        {
            var u = await UpdateByUserCoursePairAsync(useNumericWhere: true);
            if (u > 0)
            {
                return true;
            }

            if (await CountPairAsync(useNumericWhere: true) > 0)
            {
                u = await UpdateByUserCoursePairAsync(useNumericWhere: true);
                if (u > 0)
                {
                    return true;
                }
            }

            u = await UpdateByUserCoursePairAsync(useNumericWhere: false);
            if (u > 0)
            {
                return true;
            }

            if (await CountPairAsync(useNumericWhere: false) > 0)
            {
                u = await UpdateByUserCoursePairAsync(useNumericWhere: false);
                if (u > 0)
                {
                    return true;
                }
            }

            return false;
        }

        if (await TryUpdateExistingReviewAsync())
        {
            return true;
        }

        async Task<bool> TryInsertAsync(int id)
        {
        await using var insert = connection.CreateCommand();
        insert.CommandText = $"""
            INSERT INTO [{reviewsTable}] ([{reviewIdColumn}], [{reviewCourseColumn}], [{reviewUserColumn}], [{reviewRatingColumn}], [{reviewCommentColumn}], [{reviewCreatedAtColumn}])
            VALUES (?, ?, ?, ?, ?, ?)
            """;
            insert.Parameters.Add(new OleDbParameter("@id", OleDbType.Integer) { Value = id });
            insert.Parameters.Add(new OleDbParameter("@c", OleDbType.Integer) { Value = courseId });
            insert.Parameters.Add(new OleDbParameter("@u", OleDbType.Integer) { Value = userId });
            insert.Parameters.Add(new OleDbParameter("@r", OleDbType.Decimal) { Value = rating });
            insert.Parameters.Add(new OleDbParameter("@txt", OleDbType.VarWChar)
            {
                Value = commentParam,
                Size = Math.Min(4000, Math.Max(1, commentParam.Length))
            });
            insert.Parameters.Add(new OleDbParameter("@dt", OleDbType.Date) { Value = DateTime.Now });
            var ins = await insert.ExecuteNonQueryAsync(cancellationToken);
            return ins > 0;
        }

        var baseId = await GetNextReviewIdAsync(connection, reviewsTable, reviewIdColumn, cancellationToken);
        for (var bump = 0; bump < 40; bump++)
        {
            var newReviewId = baseId + bump;
            if (newReviewId <= 0)
            {
                continue;
            }

            try
            {
                if (await TryInsertAsync(newReviewId))
                {
                    return true;
                }
            }
            catch (OleDbException)
            {
                if (await TryUpdateExistingReviewAsync())
                {
                    return true;
                }

                baseId = await GetNextReviewIdAsync(connection, reviewsTable, reviewIdColumn, cancellationToken);
            }
        }

        return false;
    }

    private static CourseItem MapCourse(DbDataReader reader) =>
        new()
        {
            ID_curs = GetInt(reader, "ID_curs", "ID_cours"),
            ID_lesson = GetInt(reader, "ID_lesson", "ID_lessons"),
            ID_categorise = GetInt(reader, "ID_categorise", "ID_category"),
            price = GetDecimal(reader, "price", "Price"),
            Course_name = GetString(reader, "Course_name", "Cours_name", "course_name"),
            rating = GetDecimal(reader, "rating", "Rating"),
            create_at = GetDateTime(reader, "create_at", "created_at", "create_date"),
            preview_url = GetString(reader, "preview_url", "Preview_url", "preview"),
            teacher_user_id = GetNullableInt(reader, "teacher_user_id", "ID_teacher", "teacher_id", "teacher")
        };

    private static async Task<int> GetNextReviewIdAsync(
        OleDbConnection connection,
        string reviewsTable,
        string reviewIdColumn,
        CancellationToken cancellationToken)
    {
        var max = 0;
        await using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = $"SELECT [{reviewIdColumn}] FROM [{reviewsTable}]";
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
            while (reader is not null && await reader.ReadAsync(cancellationToken))
            {
                max = Math.Max(max, ToInt(reader[reviewIdColumn]));
            }
        }

        try
        {
            await using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = "SELECT [ID_review] FROM [USER]";
                await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
                while (reader is not null && await reader.ReadAsync(cancellationToken))
                {
                    var raw = reader["ID_review"];
                    if (raw is null || raw is DBNull)
                    {
                        continue;
                    }

                    max = Math.Max(max, ToInt(raw));
                }
            }
        }
        catch (OleDbException)
        {
            // таблица USER или поле ID_review могут отсутствовать
        }

        return max + 1;
    }

    private static async Task<int> ExecuteScalarIntAsync(OleDbConnection connection, string sql, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        var value = await command.ExecuteScalarAsync(cancellationToken);
        return ToInt(value);
    }

    private static async Task<decimal> ExecuteScalarDecimalAsync(OleDbConnection connection, string sql, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        var value = await command.ExecuteScalarAsync(cancellationToken);
        return ToDecimal(value);
    }

    private static async Task<int> GetNextEnrollmentIdAsync(
        OleDbConnection connection,
        string enrollmentTable,
        string enrollmentIdColumn,
        CancellationToken cancellationToken,
        OleDbTransaction? transaction = null)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = $"SELECT MAX([{enrollmentIdColumn}]) FROM [{enrollmentTable}]";
        var value = await command.ExecuteScalarAsync(cancellationToken);
        return Math.Max(0, ToInt(value)) + 1;
    }

    private static string? TryGetExistingTableName(OleDbConnection connection, params string[] preferredNames)
    {
        var schema = connection.GetSchema("Tables");
        var tables = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (DataRow row in schema.Rows)
        {
            var tableType = row["TABLE_TYPE"]?.ToString();
            var tableName = row["TABLE_NAME"]?.ToString();
            if (!string.Equals(tableType, "TABLE", StringComparison.OrdinalIgnoreCase) ||
                string.IsNullOrWhiteSpace(tableName))
            {
                continue;
            }

            tables.Add(tableName);
        }

        foreach (var preferredName in preferredNames)
        {
            if (tables.Contains(preferredName))
            {
                return preferredName;
            }
        }

        return null;
    }

    private static string GetExistingTableName(OleDbConnection connection, params string[] preferredNames)
    {
        var schema = connection.GetSchema("Tables");
        var tables = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (DataRow row in schema.Rows)
        {
            var tableType = row["TABLE_TYPE"]?.ToString();
            var tableName = row["TABLE_NAME"]?.ToString();
            if (!string.Equals(tableType, "TABLE", StringComparison.OrdinalIgnoreCase) ||
                string.IsNullOrWhiteSpace(tableName))
            {
                continue;
            }

            tables.Add(tableName);
        }

        foreach (var preferredName in preferredNames)
        {
            if (tables.Contains(preferredName))
            {
                return preferredName;
            }
        }

        return preferredNames[0];
    }

    private static string GetExistingColumnName(OleDbConnection connection, string tableName, params string[] preferredNames)
    {
        var schema = connection.GetSchema("Columns", new[] { null, null, tableName, null });
        var columns = new List<string>();
        var columnsSet = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (DataRow row in schema.Rows)
        {
            var columnName = row["COLUMN_NAME"]?.ToString();
            if (!string.IsNullOrWhiteSpace(columnName) && columnsSet.Add(columnName))
            {
                columns.Add(columnName);
            }
        }

        foreach (var preferredName in preferredNames)
        {
            if (columnsSet.Contains(preferredName))
            {
                return preferredName;
            }
        }

        if (columns.Count > 0)
        {
            return columns[0];
        }

        throw new InvalidOperationException($"No columns found for table '{tableName}'.");
    }

    private static string? TryGetExistingColumnName(OleDbConnection connection, string tableName, params string[] preferredNames)
    {
        var schema = connection.GetSchema("Columns", new[] { null, null, tableName, null });
        var columns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (DataRow row in schema.Rows)
        {
            var columnName = row["COLUMN_NAME"]?.ToString();
            if (!string.IsNullOrWhiteSpace(columnName))
            {
                columns.Add(columnName);
            }
        }

        foreach (var preferredName in preferredNames)
        {
            if (columns.Contains(preferredName))
            {
                return preferredName;
            }
        }

        return null;
    }

    private static int ToInt(object? value)
    {
        if (value is null || value is DBNull)
        {
            return 0;
        }

        if (value is int i)
        {
            return i;
        }

        if (value is long l)
        {
            if (l is < int.MinValue or > int.MaxValue)
            {
                return 0;
            }

            return (int)l;
        }

        if (value is decimal dec)
        {
            try
            {
                return (int)Math.Round(dec);
            }
            catch
            {
                return 0;
            }
        }

        if (value is double dbl)
        {
            try
            {
                return (int)Math.Round(dbl);
            }
            catch
            {
                return 0;
            }
        }

        var text = value.ToString()?.Trim();
        if (string.IsNullOrEmpty(text) || text.Equals("null", StringComparison.OrdinalIgnoreCase))
        {
            return 0;
        }

        if (int.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed))
        {
            return parsed;
        }

        if (double.TryParse(text.Replace(',', '.'), NumberStyles.Any, CultureInfo.InvariantCulture, out var d))
        {
            try
            {
                return (int)Math.Round(d);
            }
            catch
            {
                return 0;
            }
        }

        return 0;
    }

    private static string NormalizeEmail(string email) =>
        email.Replace("\"", string.Empty).Trim();

    private static int? ToNullableInt(object? value)
    {
        if (value is null || value is DBNull)
        {
            return null;
        }

        if (value is int i)
        {
            return i;
        }

        var text = value.ToString()?.Trim();
        if (string.IsNullOrEmpty(text) || text.Equals("null", StringComparison.OrdinalIgnoreCase))
        {
            return null;
        }

        if (int.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed))
        {
            return parsed;
        }

        if (double.TryParse(text.Replace(',', '.'), NumberStyles.Any, CultureInfo.InvariantCulture, out var d))
        {
            try
            {
                return (int)Math.Round(d);
            }
            catch
            {
                return null;
            }
        }

        return null;
    }

    private static decimal ToDecimal(object? value)
    {
        if (value is null || value is DBNull)
        {
            return 0m;
        }

        if (value is decimal decimalValue)
        {
            return decimalValue;
        }

        if (value is double doubleValue)
        {
            return Convert.ToDecimal(doubleValue);
        }

        if (value is float floatValue)
        {
            return Convert.ToDecimal(floatValue);
        }

        var text = value.ToString();
        if (string.IsNullOrWhiteSpace(text))
        {
            return 0m;
        }

        text = text.Trim().Replace(" ", string.Empty);
        if (decimal.TryParse(text, NumberStyles.Any, CultureInfo.InvariantCulture, out var invariant))
        {
            return invariant;
        }

        if (decimal.TryParse(text, NumberStyles.Any, CultureInfo.GetCultureInfo("ru-RU"), out var ru))
        {
            return ru;
        }

        text = text.Replace('.', ',');
        if (decimal.TryParse(text, NumberStyles.Any, CultureInfo.CurrentCulture, out var current))
        {
            return current;
        }

        return 0m;
    }

    private static decimal? ToNullableDecimal(object? value)
    {
        if (value is null || value is DBNull)
        {
            return null;
        }

        return ToDecimal(value);
    }

    private static DateTime ToDateTime(object? value) =>
        value is null || value is DBNull ? DateTime.MinValue : Convert.ToDateTime(value);

    private static string? NormalizeOptionalMemo(object? value)
    {
        if (value is null || value is DBNull)
        {
            return null;
        }

        var s = ToStringSafe(value);
        return string.IsNullOrEmpty(s) ? null : s;
    }

    private static string ToStringSafe(object? value)
    {
        if (value is null || value is DBNull)
        {
            return string.Empty;
        }

        var text = value.ToString() ?? string.Empty;
        return text
            .Replace("\t", " ")
            .Trim()
            .Trim('"');
    }

    private static int? GetNullableInt(DbDataReader reader, params string[] names)
    {
        foreach (var name in names)
        {
            if (TryGetValue(reader, name, out var value))
            {
                return ToNullableInt(value);
            }
        }
        return null;
    }

    private static int GetInt(DbDataReader reader, params string[] names)
    {
        foreach (var name in names)
        {
            if (TryGetValue(reader, name, out var value))
            {
                return ToInt(value);
            }
        }
        return 0;
    }

    private static decimal GetDecimal(DbDataReader reader, params string[] names)
    {
        foreach (var name in names)
        {
            if (TryGetValue(reader, name, out var value))
            {
                return ToDecimal(value);
            }
        }
        return 0m;
    }

    private static DateTime GetDateTime(DbDataReader reader, params string[] names)
    {
        foreach (var name in names)
        {
            if (TryGetValue(reader, name, out var value))
            {
                return ToDateTime(value);
            }
        }
        return DateTime.MinValue;
    }

    private static string GetString(DbDataReader reader, params string[] names)
    {
        foreach (var name in names)
        {
            if (TryGetValue(reader, name, out var value))
            {
                return ToStringSafe(value);
            }
        }
        return string.Empty;
    }

    private static bool TryGetValue(DbDataReader reader, string name, out object? value)
    {
        for (var i = 0; i < reader.FieldCount; i++)
        {
            if (string.Equals(reader.GetName(i), name, StringComparison.OrdinalIgnoreCase))
            {
                value = reader.GetValue(i);
                return true;
            }
        }

        value = null;
        return false;
    }

    private static object? GetValue(DbDataReader reader, params string[] names)
    {
        foreach (var name in names)
        {
            if (TryGetValue(reader, name, out var value))
            {
                return value;
            }
        }

        return null;
    }
}
