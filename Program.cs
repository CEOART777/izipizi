using Microsoft.AspNetCore.Authentication.Cookies;
using практика_2._0.Data;

var builder = WebApplication.CreateBuilder(args);

ConfigureAccessConnection(builder);

// Add services to the container.
builder.Services.AddRazorPages();
builder.Services.AddScoped<AccessDbService>();
builder.Services.AddHttpClient();
builder.Services
    .AddAuthentication(CookieAuthenticationDefaults.AuthenticationScheme)
    .AddCookie(options =>
    {
        options.LoginPath = "/Login";
        options.LogoutPath = "/Logout";
        options.AccessDeniedPath = "/Login";
        options.SlidingExpiration = true;
        options.ExpireTimeSpan = TimeSpan.FromDays(7);
    });
builder.Services.AddAuthorization();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Error");
    // The default HSTS value is 30 days. You may want to change this for production scenarios, see https://aka.ms/aspnetcore-hsts.
    app.UseHsts();
}

app.UseHttpsRedirection();
app.UseStaticFiles();

app.UseRouting();

app.UseAuthentication();
app.UseAuthorization();

app.MapRazorPages();

app.Run();

static void ConfigureAccessConnection(WebApplicationBuilder builder)
{
    var contentRoot = builder.Environment.ContentRootPath;
    var configuredPath = builder.Configuration["Database:Path"];
    var candidatePaths = new[]
    {
        configuredPath,
        Path.GetFullPath(Path.Combine(contentRoot, "..", "database", "Database3.accdb")),
        Path.GetFullPath(Path.Combine(contentRoot, "database", "Database3.accdb"))
    };

    var dbPath = candidatePaths
        .Where(path => !string.IsNullOrWhiteSpace(path))
        .Select(path => Path.GetFullPath(path!))
        .FirstOrDefault(File.Exists);

    if (string.IsNullOrWhiteSpace(dbPath))
    {
        Console.WriteLine("Access DB not found. Checked configured and default paths.");
        return;
    }

    Console.WriteLine($"Using Access DB: {dbPath}");
    builder.Configuration["ConnectionStrings:AccessConnection"] =
        $"Provider=Microsoft.ACE.OLEDB.12.0;Data Source={dbPath};Persist Security Info=False;";
}
