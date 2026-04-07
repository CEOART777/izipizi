using System.ComponentModel.DataAnnotations;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using практика_2._0.Data;
using практика_2._0.Models;

namespace практика_2._0.Pages;

[Authorize(Roles = "admin")]
public class AdminUserEditModel : PageModel
{
    private readonly AccessDbService _db;
    private readonly ILogger<AdminUserEditModel> _logger;

    public AdminUserEditModel(AccessDbService db, ILogger<AdminUserEditModel> logger)
    {
        _db = db;
        _logger = logger;
    }

    public UserItem? User { get; private set; }

    [BindProperty]
    public InputModel Input { get; set; } = new();

    public string? ErrorMessage { get; private set; }
    public string? SuccessMessage { get; private set; }

    public sealed class InputModel
    {
        [Required]
        public string FullName { get; set; } = string.Empty;

        [Required]
        public string Email { get; set; } = string.Empty;

        [Required]
        public string Phone { get; set; } = string.Empty;

        [Required]
        public string Role { get; set; } = "student";

        public int? BalanseCoins { get; set; }
        public decimal? BalanceRub { get; set; }

        public int RewardCoins { get; set; }

        public int? CurrentCourseId { get; set; }

        public int? HomeworkId { get; set; }

        public bool IsBlocked { get; set; }

        public DateTime? BlockedUntil { get; set; }

        public string? BlockReason { get; set; }

        public bool IsDeleted { get; set; }
    }

    public async Task OnGetAsync(int userId, CancellationToken cancellationToken)
    {
        await LoadAsync(userId, cancellationToken);
    }

    public async Task<IActionResult> OnPostAsync(int userId, CancellationToken cancellationToken)
    {
        if (!ModelState.IsValid)
        {
            ErrorMessage = "Проверьте корректность полей.";
            await LoadAsync(userId, cancellationToken);
            return Page();
        }

        // Нельзя одновременно "деактивировать" и "заблокировать" — если блокировка включена, деактивацию снимаем.
        if (Input.IsBlocked)
        {
            Input.IsDeleted = false;
        }

        try
        {
            var ok = await _db.UpdateUserAdminAsync(
                userId,
                Input.FullName.Trim(),
                Input.Email.Trim(),
                Input.Phone.Trim(),
                Input.Role.Trim(),
                Input.BalanseCoins,
                Input.BalanceRub,
                Input.RewardCoins,
                Input.CurrentCourseId,
                Input.HomeworkId,
                cancellationToken);

            // Модерация: блок/деактивация
            var deletedOk = await _db.SetUserDeletedAsync(userId, Input.IsDeleted, cancellationToken);
            var blockedOk = await _db.SetUserBlockedAsync(userId, Input.IsBlocked, Input.BlockedUntil, Input.BlockReason, cancellationToken);

            SuccessMessage = (ok && deletedOk && blockedOk) ? "Сохранено." : "Сохранено частично (проверьте поля блокировки/деактивации).";
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to update user in admin.");
            ErrorMessage = "Ошибка при сохранении в базе.";
        }

        await LoadAsync(userId, cancellationToken);
        return Page();
    }

    private async Task LoadAsync(int userId, CancellationToken cancellationToken)
    {
        User = await _db.GetUserByIdAsync(userId, cancellationToken);
        if (User is null)
        {
            return;
        }

        Input = new InputModel
        {
            FullName = User.full_name,
            Email = User.email,
            Phone = User.phone,
            Role = string.IsNullOrWhiteSpace(User.role) ? "student" : User.role,
            BalanseCoins = User.balanse_coins,
            BalanceRub = User.balance_rub,
            RewardCoins = User.reward_coins,
            CurrentCourseId = User.ID_curs,
            HomeworkId = User.ID__homework,
            IsBlocked = User.is_blocked == true,
            BlockedUntil = User.blocked_until,
            BlockReason = User.block_reason,
            IsDeleted = User.is_deleted == true
        };
    }
}

