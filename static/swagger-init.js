/* Swagger UI initializer. Lives in a static file (rather than an inline
 * <script>) so documentation pages can be served with a CSP that has no
 * 'unsafe-inline' in script-src. Configuration is passed via data attributes
 * on the #swagger-ui element. */
(function () {
  "use strict";
  var el = document.getElementById("swagger-ui");
  if (!el || typeof SwaggerUIBundle === "undefined") {
    return;
  }
  var config = el.dataset;
  window.ui = SwaggerUIBundle({
    url: config.openapiUrl,
    dom_id: "#swagger-ui",
    presets: [SwaggerUIBundle.presets.apis, SwaggerUIBundle.presets.standalone],
    docExpansion: config.docExpansion,
    persistAuthorization: config.persistAuth === "true",
    defaultModelsExpandDepth: parseInt(config.modelsExpandDepth, 10),
    defaultModelExpandDepth: parseInt(config.modelExpandDepth, 10),
  });
})();
