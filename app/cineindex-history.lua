-- cineindex-history.lua
-- Logs file-loaded events as JSON lines to a history file.
-- The path MUST be provided via the CINEINDEX_HISTORY_PATH environment variable
-- (set by the Python app). If it's missing, the script disables itself.

local function get_logfile()
    local env_path = os.getenv("CINEINDEX_HISTORY_PATH")
    if env_path == nil or env_path == "" then
        mp.msg.error(
            "cineindex-history.lua: CINEINDEX_HISTORY_PATH is not set; history logging disabled"
        )
        return nil
    end
    return env_path
end

local logfile = get_logfile()
if not logfile then
    -- Disable script if don't have an explicit path
    return
end

local function json_escape(str)
    if not str then
        return ""
    end
    str = str:gsub("\\", "\\\\")
    str = str:gsub("\"", "\\\"")
    str = str:gsub("\n", "\\n")
    str = str:gsub("\r", "\\r")
    return str
end

local function write_event()
    local path = mp.get_property("path") or ""
    local title = mp.get_property("media-title") or ""
    local t = os.date("%Y-%m-%d %H:%M:%S")

    title = json_escape(title)
    path = json_escape(path)

    local line = string.format('{"Name":"%s","Url":"%s","Time":"%s"}\n', title, path, t)

    local f, err = io.open(logfile, "a")
    if f then
        f:write(line)
        f:close()
    else
        mp.msg.warn("cineindex-history.lua: failed to open log file: " .. tostring(err))
    end
end

mp.register_event("file-loaded", write_event)
mp.msg.info("cineindex-history.lua loaded; logging to: " .. logfile)
