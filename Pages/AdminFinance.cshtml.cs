using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc.RazorPages;
using практика_2._0.Data;
using практика_2._0.Models;

namespace практика_2._0.Pages;

[Authorize(Roles = "admin")]
public class AdminFinanceModel : PageModel
{
    private readonly AccessDbService _db;
    private readonly ILogger<AdminFinanceModel> _logger;

    public AdminFinanceModel(AccessDbService db, ILogger<AdminFinanceModel> logger)
    {
        _db = db;
        _logger = logger;
    }

    public IReadOnlyList<PaymentItem> Payments { get; private set; } = [];
    public IReadOnlyList<EnrollmentItem> Enrollments { get; private set; } = [];

    public int UniquePayUsers { get; private set; }
    public decimal TotalPaidAmount { get; private set; }
    public string TopPaidCourseName { get; private set; } = "—";
    public int TopPaidCourseCount { get; private set; }
    public IReadOnlyList<(string CourseName, int Count)> PaymentsByCourse { get; private set; } = [];
    public IReadOnlyList<(string CourseName, int Count)> EnrollmentsByCourse { get; private set; } = [];

    public async Task OnGetAsync(CancellationToken cancellationToken)
    {
        try
        {
            Enrollments = await _db.GetAllEnrollmentsAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to load enrollments for admin.");
            Enrollments = [];
        }

        // Восстановление PAY из ENROLLMENTS (на случай, если ранее PAY не заполнялся).
        try
        {
            await _db.BackfillPayFromEnrollmentsAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to backfill PAY from ENROLLMENTS.");
        }

        try
        {
            Payments = await _db.GetAllPaymentsAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to load payments for admin.");
            Payments = [];
        }

        try
        {
            var courses = await _db.GetCoursesAsync(cancellationToken);
            var courseNames = courses.ToDictionary(c => c.ID_curs, c => c.Course_name);

            UniquePayUsers = Payments.Select(p => p.ID_user).Where(x => x > 0).Distinct().Count();
            TotalPaidAmount = await _db.GetTotalPaidRevenueRobustAsync(cancellationToken);

            PaymentsByCourse = Payments
                .Where(p => p.ID_curs > 0)
                .GroupBy(p => p.ID_curs)
                .Select(g => (CourseName: courseNames.TryGetValue(g.Key, out var n) ? n : $"Курс #{g.Key}", Count: g.Count()))
                .OrderByDescending(x => x.Count)
                .ToList();

            var topPay = PaymentsByCourse.FirstOrDefault();
            if (!string.IsNullOrWhiteSpace(topPay.CourseName))
            {
                TopPaidCourseName = topPay.CourseName;
                TopPaidCourseCount = topPay.Count;
            }

            EnrollmentsByCourse = Enrollments
                .Where(e => e.ID_cours > 0)
                .GroupBy(e => e.ID_cours)
                .Select(g => (CourseName: courseNames.TryGetValue(g.Key, out var n) ? n : $"Курс #{g.Key}", Count: g.Count()))
                .OrderByDescending(x => x.Count)
                .ToList();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to build finance aggregates.");
            PaymentsByCourse = [];
            EnrollmentsByCourse = [];
        }
    }
}

