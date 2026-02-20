package pl.bpiatek.linkshortenerredirectservice.link;

public record RedirectInfo(String longUrl, boolean isActive, long updatedAtMicros, boolean isDeleted) {
}
