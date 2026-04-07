namespace практика_2._0.Models;

public sealed class AdminDashboardVm
{
    public AdminStatsItem Stats { get; init; } = new();

    public IReadOnlyList<string> Months { get; init; } = Array.Empty<string>();
    public IReadOnlyList<decimal> RevenueByMonth { get; init; } = Array.Empty<decimal>();
    public IReadOnlyList<int> NewUsersByMonth { get; init; } = Array.Empty<int>();
}

