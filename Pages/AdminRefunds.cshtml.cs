using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using практика_2._0.Data;

namespace практика_2._0.Pages;

[Authorize(Roles = "admin")]
public class AdminRefundsModel : PageModel
{
    private readonly AccessDbService _db;
    private readonly ILogger<AdminRefundsModel> _logger;

    public AdminRefundsModel(AccessDbService db, ILogger<AdminRefundsModel> logger)
    {
        _db = db;
        _logger = logger;
    }

    public sealed class RefundRequestRow
    {
        public int EnrollmentId { get; init; }
        public int UserId { get; init; }
        public string UserName { get; init; } = string.Empty;
        public string UserEmail { get; init; } = string.Empty;
        public int CourseId { get; init; }
        public string CourseName { get; init; } = string.Empty;
        public decimal Amount { get; init; }
        public string Status { get; init; } = string.Empty;
        public int ProgressPercent { get; init; }
    }

    public IReadOnlyList<RefundRequestRow> Requests { get; private set; } = [];
    public string? SuccessMessage { get; private set; }
    public string? ErrorMessage { get; private set; }

    public async Task OnGetAsync(CancellationToken cancellationToken)
    {
        await LoadAsync(cancellationToken);
    }

    public async Task<IActionResult> OnPostApproveAsync(int enrollmentId, int userId, int courseId, CancellationToken cancellationToken)
    {
        if (enrollmentId <= 0 || userId <= 0 || courseId <= 0)
        {
            ErrorMessage = "Некорректная заявка.";
            await LoadAsync(cancellationToken);
            return Page();
        }

        try
        {
            var ok = await _db.ApproveRefundForEnrollmentAsync(enrollmentId, userId, courseId, cancellationToken);
            if (!ok)
            {
                ErrorMessage = "Не удалось подтвердить отмену/возврат (метод вернул false).";
            }
            else
            {
                SuccessMessage = "Отмена подтверждена, средства возвращены пользователю.";
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to approve refund request.");
            ErrorMessage = $"Ошибка при подтверждении возврата: {ex.Message}";
        }

        await LoadAsync(cancellationToken);
        return Page();
    }

    public async Task<IActionResult> OnPostRejectAsync(int enrollmentId, int userId, int courseId, CancellationToken cancellationToken)
    {
        if (enrollmentId <= 0 || userId <= 0 || courseId <= 0)
        {
            ErrorMessage = "Некорректная заявка.";
            await LoadAsync(cancellationToken);
            return Page();
        }

        try
        {
            var ok = await _db.RejectRefundForEnrollmentAsync(enrollmentId, userId, courseId, cancellationToken);
            if (!ok)
            {
                ErrorMessage = "Не удалось отклонить заявку.";
            }
            else
            {
                SuccessMessage = "Заявка отклонена. Подписка восстановлена.";
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to reject refund request.");
            ErrorMessage = "Ошибка при отклонении заявки.";
        }

        await LoadAsync(cancellationToken);
        return Page();
    }

    private async Task LoadAsync(CancellationToken cancellationToken)
    {
        try
        {
            var requested = (await _db.GetRefundRequestedEnrollmentsAsync(cancellationToken))
                .OrderByDescending(e => e.ID_enrolment)
                .ToList();
            // В старых/грязных данных может быть несколько записей по одному курсу (после повторных оплат/возвратов).
            // В админке показываем одну "актуальную" заявку на (пользователь + курс).
            requested = requested
                .GroupBy(e => (e.ID_user, e.ID_cours))
                .Select(g => g.OrderByDescending(x => x.ID_enrolment).First())
                .OrderByDescending(e => e.ID_enrolment)
                .ToList();

            var rows = new List<RefundRequestRow>(requested.Count);
            foreach (var e in requested)
            {
                // Не даём отмене HTTP-запроса ронять загрузку (OleDb).
                var user = await _db.GetUserByIdAsync(e.ID_user, CancellationToken.None);
                var course = await _db.GetCourseByIdAsync(e.ID_cours, CancellationToken.None);
                var progressPercent = await _db.GetCourseProgressPercentForUserAsync(e.ID_user, e.ID_cours, CancellationToken.None);
                var amount = e.price > 0 ? e.price : (course?.price ?? 0m);
                rows.Add(new RefundRequestRow
                {
                    EnrollmentId = e.ID_enrolment,
                    UserId = e.ID_user,
                    UserName = user?.full_name ?? $"User #{e.ID_user}",
                    UserEmail = user?.email ?? string.Empty,
                    CourseId = e.ID_cours,
                    CourseName = course?.Course_name ?? $"Курс #{e.ID_cours}",
                    Amount = amount,
                    Status = e.status,
                    ProgressPercent = progressPercent
                });
            }

            Requests = rows;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to load refund requests.");
            Requests = [];
            if (string.IsNullOrWhiteSpace(ErrorMessage))
            {
                ErrorMessage = "Не удалось загрузить заявки на возврат.";
            }
        }
    }
}
