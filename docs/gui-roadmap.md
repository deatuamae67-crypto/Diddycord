# Graphical application roadmap

Diddycord's GUI is being introduced in layers so the existing bounded networking core and legacy-platform guarantees remain testable at every merge.

1. **Desktop shell (current chapter):** native window, bot login, connection state, guild/DM navigation, channel timelines, history bootstrap and message sending.
2. **Desktop interaction parity:** unread/activity markers, message edit/delete actions, reconnect/error UX, richer channel grouping and keyboard interaction.
3. **Android application package:** NativeActivity-based graphical frontend targeting Android API 23+, packaged as an installable APK while reusing the same Rust core.
4. **Release packaging:** replace the current Android native-command asset with the APK and keep downloadable Windows/Linux graphical binaries.
5. **Polish:** persistent non-secret preferences, icons, DPI/touch tuning, accessibility and low-end performance profiling.

The project remains bot-token only throughout this roadmap.
