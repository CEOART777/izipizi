namespace практика_2._0.Models;

/// <summary>Навигация по разделам профиля и краткие показатели баллов.</summary>
public sealed class ProfileSectionNavModel
{
    /// <summary>info | security | notifications</summary>
    public string Active { get; init; } = "info";

    public int UserId { get; init; }

    public int? BonusBalance { get; init; }

    public decimal? RubBalance { get; init; }

    public int RewardCoins { get; init; }
}
