package whelk.rest.api;

import whelk.JsonLd;
import whelk.util.http.BadRequestException;
import whelk.util.http.MimeTypes;
import whelk.util.http.NotFoundException;

import javax.servlet.http.HttpServletRequest;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static whelk.rest.api.CrudUtils.*;

class CrudGetRequest {
    private final HttpServletRequest request;
    private String resourceId;
    private String dataLeaf;
    private final String contentType;
    private View view;
    private final Lens lens;
    private final String profile;
    private final String computedLabelLocale;
    private final boolean findBlank;

    private static final Pattern PATH_PATTERN = Pattern.compile("^/(.+?)(/(data|data-view|_changesets)(\\.(\\w+))?)?$");

    static CrudGetRequest parse(HttpServletRequest request) {
        return new CrudGetRequest(request);
    }

    private CrudGetRequest(HttpServletRequest request) {
        this.request = request;
        parsePath(getPath());
        contentType = getBestContentType(getAcceptHeader(request), dataLeaf);
        lens = parseLens(request);
        profile = parseProfile(request);

        computedLabelLocale = parseComputedLabelLocale(request).orElse(null);
        findBlank = Optional.ofNullable(request.getParameter("_findBlank")).map("true"::equalsIgnoreCase).orElse(false);
    }

    HttpServletRequest getHttpServletRequest() {
        return request;
    }

    String getPath() {
        return Crud.getRequestPath(request);
    }

    String getId() {
        return resourceId;
    }

    Optional<String> getVersion() {
        return Optional.ofNullable(request.getParameter("version"));
    }

    Optional<ETag> getIfNoneMatch() {
        return CrudUtils.getIfNoneMatch(request);
    }

    String getContentType() {
        return contentType;
    }

    boolean shouldEmbellish() {
        if (getVersion().isPresent() && !getBoolParameter("embellished").orElse(false)) {
            return false;
        }

        return getBoolParameter("embellished").orElse(true);
    }

    boolean shouldFrame() {
        return getBoolParameter("framed").orElse(MimeTypes.JSON.equals(contentType));
    }

    boolean shouldApplyInverseOf() {
        return getBoolParameter("_applyInverseOf").orElse(false);
    }

    boolean shouldComputeLabels() {
        return computedLabelLocale != null;
    }

    boolean shouldGenerateBlankFindLinks() {
        return findBlank;
    }

    String computedLabelLocale() {
        return computedLabelLocale;
    }

    View getView() {
        return view;
    }

    Lens getLens() {
        return lens;
    }

    Optional<String> getProfile() {
        return Optional.ofNullable(profile);
    }

    /**
     * Parse a CRUD path
     *
     * Matches /<id>, /<id>/<view> and /<id>/<view>.<suffix>
     * where view is 'data' or 'data-view'
     */
    private void parsePath(String path) {
        Matcher matcher = PATH_PATTERN.matcher(path);
        if (matcher.matches()) {
            resourceId = matcher.group(1);
            dataLeaf = matcher.group(2);
            view = View.fromString(matcher.group(3));
        } else {
            throw new NotFoundException("Not found:" + path);
        }
    }

    private Lens parseLens(HttpServletRequest request) {
        String lens = request.getParameter("lens");
        if (lens == null) {
            return Lens.NONE;
        }
        try {
            return Lens.valueOf(lens.toUpperCase());
        }
        catch (IllegalArgumentException e) {
            throw new BadRequestException("Unknown lens:" + lens);
        }
    }

    private String parseProfile(HttpServletRequest request) {
        String param = request.getParameter("profile");
        if (param != null) {
            return param;
        }
        String header = request.getHeader("Accept-Profile");
        if (header != null) {
            header = header.trim();
            boolean startAngle = header.startsWith("<");
            boolean endAngle = header.endsWith(">");
            if (startAngle || endAngle) {
                int start = startAngle ? 1 : 0;
                int end = header.length() - (endAngle ? 1 : 0);
                header = header.substring(start, end);
            }
            return header;
        }
        return null;
    }

    static Optional<String> parseComputedLabelLocale(HttpServletRequest request) {
        return Optional.ofNullable(request.getParameter(JsonLd.Platform.COMPUTED_LABEL));
    }

    private Optional<Boolean> getBoolParameter(String name) {
        Object param = request.getParameter(name);
        if (param == null) {
            param = request.getAttribute(name);
        }
        return Optional
                .ofNullable(param)
                .map(o -> Boolean.parseBoolean(o.toString()));
    }

    enum View {
        RESOURCE(""),
        DATA("data"),
        DATA_VIEW("data-view"),
        CHANGE_SETS("_changesets");

        private final String name;

        View(String name) {
            this.name = name;
        }

        static View fromString(String s) {
            for (View v : values()) {
                if (v.name.equals(s)) {
                    return v;
                }
            }
            return RESOURCE;
        }
    }

    enum Lens {
        CHIP, CARD, NONE
    }
}
