/**
 * Toth Intelligence — Attribution Script
 *
 * Insira este script no tema do Shopify (theme.liquid, antes do </body>)
 * Ele captura click IDs (fbc, fbp, gclid) e UTMs quando o visitante chega,
 * e adiciona esses dados ao link de WhatsApp quando o visitante clica.
 *
 * Resultado: O Nexus recebe a mensagem com dados de atribuição,
 * e o Intelligence pode enviar conversões com fbc/gclid para Meta/Google.
 */
(function() {
  'use strict';

  // === 1. CAPTURAR DADOS DE ATRIBUIÇÃO NA CHEGADA ===

  var params = new URLSearchParams(window.location.search);
  var attribution = JSON.parse(localStorage.getItem('toth_attribution') || '{}');

  // Capturar UTMs (first-touch: só grava se não existir)
  var utmFields = ['utm_source', 'utm_medium', 'utm_campaign', 'utm_content', 'utm_term', 'utm_ad_id', 'utm_adset_id', 'utm_campaign_id'];
  if (!attribution.utm_source) {
    utmFields.forEach(function(field) {
      var val = params.get(field);
      if (val) attribution[field] = val;
    });
  }

  // Capturar fbclid (Meta click ID)
  var fbclid = params.get('fbclid');
  if (fbclid) {
    attribution.fbclid = fbclid;
    // Gerar fbc cookie no formato Meta: fb.{subdomainIndex}.{creationTime}.{fbclid}
    var fbc = 'fb.2.' + Date.now() + '.' + fbclid;
    attribution.fbc = fbc;
    document.cookie = '_fbc=' + fbc + '; path=/; max-age=' + (90 * 86400) + '; SameSite=Lax';
  }

  // Capturar gclid (Google click ID)
  var gclid = params.get('gclid');
  if (gclid) {
    attribution.gclid = gclid;
  }

  // Capturar fbp (Meta browser ID) se existir no cookie
  var fbpMatch = document.cookie.match(/_fbp=([^;]+)/);
  if (fbpMatch) attribution.fbp = fbpMatch[1];

  // Capturar fbc do cookie se existir (pode ter sido setado pelo Pixel)
  var fbcMatch = document.cookie.match(/_fbc=([^;]+)/);
  if (fbcMatch && !attribution.fbc) attribution.fbc = fbcMatch[1];

  // Salvar timestamp da visita
  if (!attribution.firstVisit) attribution.firstVisit = new Date().toISOString();
  attribution.lastVisit = new Date().toISOString();
  attribution.pageUrl = window.location.pathname;

  // Persistir
  localStorage.setItem('toth_attribution', JSON.stringify(attribution));

  // === 2. INTERCEPTAR CLIQUES NO WHATSAPP ===

  document.addEventListener('click', function(e) {
    var link = e.target.closest('a[href*="whatsapp"], a[href*="wa.me"]');
    if (!link) return;

    e.preventDefault();

    // Recuperar dados de atribuição
    var attr = JSON.parse(localStorage.getItem('toth_attribution') || '{}');

    // Construir mensagem com dados de atribuição (invisível para o cliente)
    var baseUrl = link.href;
    var productName = document.querySelector('h1.product__title, h1[class*="product"], .product-single__title')?.textContent?.trim() || '';
    var productPrice = document.querySelector('.product__price .money, .price .money, [class*="price"] .money')?.textContent?.trim() || '';

    // Construir texto da mensagem com dados escondidos no final
    var originalText = new URLSearchParams(new URL(baseUrl).search).get('text') || '';

    // Adicionar dados de atribuição como parâmetros UTM na URL de retorno
    var attrParams = [];
    if (attr.utm_source) attrParams.push('src=' + encodeURIComponent(attr.utm_source));
    if (attr.utm_campaign) attrParams.push('cmp=' + encodeURIComponent(attr.utm_campaign));
    if (attr.fbc) attrParams.push('fbc=' + encodeURIComponent(attr.fbc));
    if (attr.fbp) attrParams.push('fbp=' + encodeURIComponent(attr.fbp));
    if (attr.gclid) attrParams.push('gclid=' + encodeURIComponent(attr.gclid));
    if (productName) attrParams.push('prod=' + encodeURIComponent(productName.substring(0, 50)));

    // Construir URL final
    var waUrl = baseUrl;
    if (attrParams.length > 0) {
      var attrString = attrParams.join('&');
      // Adicionar como texto codificado na mensagem
      var messageText = originalText || ('Olá! Vi o produto ' + (productName || 'no site') + ' e gostaria de mais informações.');
      messageText += '\n\n---\nref:' + btoa(attrString).substring(0, 60);

      // Reconstruir URL do WhatsApp
      var waBase = baseUrl.split('?')[0];
      waUrl = waBase + '?phone=5518996714293&text=' + encodeURIComponent(messageText);
    }

    // Disparar evento de conversão no Pixel antes de redirecionar
    if (typeof fbq !== 'undefined') {
      fbq('trackCustom', 'ClickWhatsApp', {
        content_name: productName,
        value: productPrice,
        fbc: attr.fbc || '',
        fbp: attr.fbp || ''
      });
    }

    // Disparar evento no Google
    if (typeof gtag !== 'undefined') {
      gtag('event', 'click_whatsapp', {
        event_category: 'engagement',
        event_label: productName,
        value: productPrice
      });
    }

    // Navegar para WhatsApp
    window.open(waUrl, '_blank');

  }, true);

  // === 3. ENVIAR DADOS PARA INTELLIGENCE (opcional — para tracking server-side) ===

  // Se Intelligence estiver configurado, envia beacon com dados de atribuição
  if (attribution.fbc || attribution.gclid || attribution.utm_source) {
    try {
      navigator.sendBeacon('/apps/intelligence-track', JSON.stringify({
        event: 'page_view_attributed',
        fbc: attribution.fbc,
        fbp: attribution.fbp,
        gclid: attribution.gclid,
        utm_source: attribution.utm_source,
        utm_campaign: attribution.utm_campaign,
        url: window.location.pathname
      }));
    } catch(e) {}
  }

})();
