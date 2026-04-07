using Microsoft.AspNetCore.Mvc.RazorPages;
using практика_2._0.Data;

namespace практика_2._0.Pages;

public class MaterialViewerModel : PageModel
{
    private readonly AccessDbService _accessDbService;

    public MaterialViewerModel(AccessDbService accessDbService)
    {
        _accessDbService = accessDbService;
    }

    public int LessonId { get; private set; }
    public string LessonTitle { get; private set; } = string.Empty;
    public string? OpenUrl { get; private set; }
    public string? DownloadUrl { get; private set; }
    public bool CanInlineView { get; private set; }
    public string? ErrorMessage { get; private set; }

    public async Task OnGetAsync(int lessonId, CancellationToken cancellationToken)
    {
        LessonId = lessonId;
        if (lessonId <= 0)
        {
            ErrorMessage = "Некорректный урок.";
            return;
        }

        var lesson = await _accessDbService.GetLessonByIdAsync(lessonId, cancellationToken);
        if (lesson is null)
        {
            ErrorMessage = "Урок не найден.";
            return;
        }

        LessonTitle = lesson.Cours_name;

        // Открытие/скачивание делаем через handlers страницы урока, чтобы поддерживались D:\..., /materials/..., https://...
        OpenUrl = $"/Lesson?handler=Material&lessonId={lessonId}";
        DownloadUrl = $"/Lesson?handler=MaterialDownload&lessonId={lessonId}";

        var raw = lesson.meterials_url ?? string.Empty;
        CanInlineView = CanInlineByExtension(raw);
    }

    private static bool CanInlineByExtension(string raw)
    {
        var s = (raw ?? string.Empty).Trim().Trim('"').Trim();
        // если в строке есть http(s), берём хвост
        var httpsIdx = s.IndexOf("https://", StringComparison.OrdinalIgnoreCase);
        var httpIdx = s.IndexOf("http://", StringComparison.OrdinalIgnoreCase);
        var idx = httpsIdx >= 0 ? httpsIdx : httpIdx;
        if (idx >= 0)
        {
            s = s[idx..];
        }

        var ext = Path.GetExtension(s).ToLowerInvariant();
        return ext is ".pdf" or ".png" or ".jpg" or ".jpeg" or ".gif" or ".webp" or ".txt";
    }
}

