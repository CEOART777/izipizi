using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using практика_2._0.Data;
using практика_2._0.Models;

namespace практика_2._0.Pages;

[Authorize(Roles = "admin")]
public class AdminTeacherAssignModel : PageModel
{
    private readonly AccessDbService _db;
    private readonly ILogger<AdminTeacherAssignModel> _logger;

    public AdminTeacherAssignModel(AccessDbService db, ILogger<AdminTeacherAssignModel> logger)
    {
        _db = db;
        _logger = logger;
    }

    [BindProperty(SupportsGet = true)]
    public int TeacherId { get; set; }

    public UserItem? Teacher { get; private set; }
    public IReadOnlyList<CourseItem> Courses { get; private set; } = [];
    public IReadOnlySet<int> AssignedCourseIds { get; private set; } = new HashSet<int>();
    public IReadOnlyDictionary<int, int> CourseTeacherMap { get; private set; } = new Dictionary<int, int>();

    public string? ErrorMessage { get; private set; }
    public string? SuccessMessage { get; private set; }

    public async Task OnGetAsync(CancellationToken cancellationToken)
    {
        await LoadAsync(cancellationToken);
    }

    public async Task<IActionResult> OnPostAssignAsync(int teacherId, int courseId, CancellationToken cancellationToken)
    {
        TeacherId = teacherId;
        if (teacherId <= 0 || courseId <= 0)
        {
            ErrorMessage = "Некорректные данные.";
            await LoadAsync(cancellationToken);
            return Page();
        }

        try
        {
            var ok = await _db.AssignTeacherToCourseAsync(teacherId, courseId, cancellationToken);
            SuccessMessage = ok ? "Учитель назначен." : "Не удалось назначить учителя.";
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to assign teacher to course.");
            ErrorMessage = "Ошибка при сохранении в базе.";
        }

        await LoadAsync(cancellationToken);
        return Page();
    }

    public async Task<IActionResult> OnPostUnassignAsync(int teacherId, int courseId, CancellationToken cancellationToken)
    {
        TeacherId = teacherId;
        if (teacherId <= 0 || courseId <= 0)
        {
            ErrorMessage = "Некорректные данные.";
            await LoadAsync(cancellationToken);
            return Page();
        }

        try
        {
            var ok = await _db.UnassignTeacherFromCourseAsync(teacherId, courseId, cancellationToken);
            SuccessMessage = ok ? "Назначение снято." : "Не удалось снять назначение.";
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to unassign teacher from course.");
            ErrorMessage = "Ошибка при сохранении в базе.";
        }

        await LoadAsync(cancellationToken);
        return Page();
    }

    public async Task<IActionResult> OnPostUnassignAllAsync(int teacherId, CancellationToken cancellationToken)
    {
        TeacherId = teacherId;
        if (teacherId <= 0)
        {
            ErrorMessage = "Некорректные данные.";
            await LoadAsync(cancellationToken);
            return Page();
        }

        try
        {
            var ok = await _db.UnassignTeacherFromAllCoursesAsync(teacherId, cancellationToken);
            SuccessMessage = ok ? "Все назначения сняты." : "Не удалось снять назначения.";
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to unassign teacher from all courses.");
            ErrorMessage = "Ошибка при сохранении в базе.";
        }

        await LoadAsync(cancellationToken);
        return Page();
    }

    private async Task LoadAsync(CancellationToken cancellationToken)
    {
        if (TeacherId <= 0)
        {
            ErrorMessage = "Не выбран учитель.";
            Teacher = null;
            Courses = [];
            return;
        }

        Teacher = await _db.GetUserByIdAsync(TeacherId, cancellationToken);
        if (Teacher is null)
        {
            Courses = [];
            return;
        }

        Courses = await _db.GetCoursesAsync(cancellationToken);
        AssignedCourseIds = await _db.GetAssignedCourseIdsForTeacherAsync(TeacherId, cancellationToken);
        var map = new Dictionary<int, int>();

        // 1) из самих курсов
        foreach (var c in Courses)
        {
            if (c.teacher_user_id is int tid && tid > 0 && !map.ContainsKey(c.ID_curs))
            {
                map[c.ID_curs] = tid;
            }
        }

        // 2) из TEACHER_COURSES
        var mapRows = await _db.GetTeacherCourseAssignmentsAsync(cancellationToken);
        foreach (var kv in mapRows)
        {
            if (!map.ContainsKey(kv.Key))
            {
                map[kv.Key] = kv.Value;
            }
        }

        CourseTeacherMap = map;
    }
}

