using System.ComponentModel.DataAnnotations;

namespace TMech.Gateway;

internal sealed record Config
{
    [Required(AllowEmptyStrings = false)]
    public required string Host { get; init; } = string.Empty;

    [Required]
    [Range(10000, ushort.MaxValue)]
    public required int Port { get; init; }
}
