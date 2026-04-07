using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc.RazorPages;
using практика_2._0.Data;
using практика_2._0.Models;

namespace практика_2._0.Pages;

    [Authorize(Roles = "admin")]
public class AdminModel : PageModel
{
    private readonly AccessDbService _accessDbService;
    private readonly ILogger<AdminModel> _logger;

    public AdminModel(AccessDbService accessDbService, ILogger<AdminModel> logger)
    {
        _accessDbService = accessDbService;
        _logger = logger;
    }

    public AdminDashboardVm Dashboard { get; private set; } = new();

    public async Task OnGetAsync(CancellationToken cancellationToken)
    {
        try
        {
            Dashboard = await _accessDbService.GetAdminDashboardAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to load admin stats.");
            Dashboard = new AdminDashboardVm();
        }
    }
}
