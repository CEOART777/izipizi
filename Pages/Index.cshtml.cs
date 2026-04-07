using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using практика_2._0.Data;
using практика_2._0.Models;

namespace практика_2._0.Pages
{
    public class IndexModel : PageModel
    {
        private readonly ILogger<IndexModel> _logger;
        private readonly AccessDbService _accessDbService;

        public IndexModel(ILogger<IndexModel> logger, AccessDbService accessDbService)
        {
            _logger = logger;
            _accessDbService = accessDbService;
        }

        public IReadOnlyList<CategoryItem> Categories { get; private set; } = [];
        public IReadOnlyList<CourseItem> Courses { get; private set; } = [];
        public IReadOnlyDictionary<int, string> CategoryNames { get; private set; } = new Dictionary<int, string>();
        [BindProperty(SupportsGet = true)]
        public int? CategoryId { get; set; }
        [BindProperty(SupportsGet = true)]
        public string? Search { get; set; }
        [BindProperty(SupportsGet = true)]
        public string? SortBy { get; set; }

        public async Task OnGetAsync(CancellationToken cancellationToken)
        {
            try
            {
                Categories = await _accessDbService.GetCategoriesAsync(cancellationToken);
                CategoryNames = Categories.ToDictionary(c => c.ID_categorise, c => c.name_categori);
                var courses = await _accessDbService.GetCoursesAsync(cancellationToken);

                if (CategoryId.HasValue)
                {
                    courses = courses.Where(c => c.ID_categorise == CategoryId.Value).ToList();
                }

                if (!string.IsNullOrWhiteSpace(Search))
                {
                    var search = Search.Trim();
                    courses = courses
                        .Where(c => c.Course_name.Contains(search, StringComparison.OrdinalIgnoreCase))
                        .ToList();
                }

                courses = (SortBy ?? "rating_desc") switch
                {
                    "price_asc" => courses.OrderBy(c => c.price).ToList(),
                    "price_desc" => courses.OrderByDescending(c => c.price).ToList(),
                    "rating_asc" => courses.OrderBy(c => c.rating).ToList(),
                    "newest" => courses.OrderByDescending(c => c.create_at).ToList(),
                    _ => courses.OrderByDescending(c => c.rating).ToList()
                };

                Courses = courses;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to load home page data from Access database.");
                Categories = [];
                Courses = [];
            }
        }
    }
}
