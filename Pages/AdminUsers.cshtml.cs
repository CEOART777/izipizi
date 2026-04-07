using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc.RazorPages;
using практика_2._0.Data;
using практика_2._0.Models;

namespace практика_2._0.Pages;

[Authorize(Roles = "admin")]
public class AdminUsersModel : PageModel
{
    private readonly AccessDbService _db;
    private readonly ILogger<AdminUsersModel> _logger;

    public AdminUsersModel(AccessDbService db, ILogger<AdminUsersModel> logger)
    {
        _db = db;
        _logger = logger;
    }

    public IReadOnlyList<UserItem> Users { get; private set; } = [];

    public async Task OnGetAsync(CancellationToken cancellationToken)
    {
        try
        {
            Users = await _db.GetAllUsersAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to load users for admin.");
            Users = [];
        }
    }
}

