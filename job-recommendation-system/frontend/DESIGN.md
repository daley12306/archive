---
name: JobAI Core
colors:
  surface: '#f9f9ff'
  surface-dim: '#d3daef'
  surface-bright: '#f9f9ff'
  surface-container-lowest: '#ffffff'
  surface-container-low: '#f1f3ff'
  surface-container: '#e9edff'
  surface-container-high: '#e1e8fd'
  surface-container-highest: '#dce2f7'
  on-surface: '#141b2b'
  on-surface-variant: '#434655'
  inverse-surface: '#293040'
  inverse-on-surface: '#edf0ff'
  outline: '#737686'
  outline-variant: '#c3c6d7'
  surface-tint: '#0053db'
  primary: '#004ac6'
  on-primary: '#ffffff'
  primary-container: '#2563eb'
  on-primary-container: '#eeefff'
  inverse-primary: '#b4c5ff'
  secondary: '#585f67'
  on-secondary: '#ffffff'
  secondary-container: '#dce3ec'
  on-secondary-container: '#5e656d'
  tertiary: '#943700'
  on-tertiary: '#ffffff'
  tertiary-container: '#bc4800'
  on-tertiary-container: '#ffede6'
  error: '#ba1a1a'
  on-error: '#ffffff'
  error-container: '#ffdad6'
  on-error-container: '#93000a'
  primary-fixed: '#dbe1ff'
  primary-fixed-dim: '#b4c5ff'
  on-primary-fixed: '#00174b'
  on-primary-fixed-variant: '#003ea8'
  secondary-fixed: '#dce3ec'
  secondary-fixed-dim: '#c0c7d0'
  on-secondary-fixed: '#151c23'
  on-secondary-fixed-variant: '#40484f'
  tertiary-fixed: '#ffdbcd'
  tertiary-fixed-dim: '#ffb596'
  on-tertiary-fixed: '#360f00'
  on-tertiary-fixed-variant: '#7d2d00'
  background: '#f9f9ff'
  on-background: '#141b2b'
  surface-variant: '#dce2f7'
typography:
  display-lg:
    fontFamily: Inter
    fontSize: 48px
    fontWeight: '700'
    lineHeight: 56px
    letterSpacing: -0.02em
  headline-lg:
    fontFamily: Inter
    fontSize: 32px
    fontWeight: '600'
    lineHeight: 40px
    letterSpacing: -0.01em
  headline-lg-mobile:
    fontFamily: Inter
    fontSize: 24px
    fontWeight: '600'
    lineHeight: 32px
  headline-md:
    fontFamily: Inter
    fontSize: 24px
    fontWeight: '600'
    lineHeight: 32px
  headline-sm:
    fontFamily: Inter
    fontSize: 20px
    fontWeight: '600'
    lineHeight: 28px
  body-lg:
    fontFamily: Inter
    fontSize: 18px
    fontWeight: '400'
    lineHeight: 28px
  body-md:
    fontFamily: Inter
    fontSize: 16px
    fontWeight: '400'
    lineHeight: 24px
  body-sm:
    fontFamily: Inter
    fontSize: 14px
    fontWeight: '400'
    lineHeight: 20px
  label-md:
    fontFamily: Inter
    fontSize: 14px
    fontWeight: '500'
    lineHeight: 20px
  label-sm:
    fontFamily: Inter
    fontSize: 12px
    fontWeight: '600'
    lineHeight: 16px
rounded:
  sm: 0.25rem
  DEFAULT: 0.5rem
  md: 0.75rem
  lg: 1rem
  xl: 1.5rem
  full: 9999px
spacing:
  container-max: 1280px
  gutter: 24px
  margin-desktop: 40px
  margin-mobile: 16px
  stack-sm: 8px
  stack-md: 16px
  stack-lg: 32px
---

## Brand & Style

This design system is built on a **Corporate / Modern** aesthetic, emphasizing clarity, intelligence, and efficiency. The personality is professional and trustworthy, utilizing a clean "SaaS-plus" look that balances a neutral foundation with vibrant, purposeful AI accents. 

The visual language focuses on high legibility and a systematic approach to information density. It avoids unnecessary decoration, opting instead for functional elevation and a crisp, light-themed interface that reduces cognitive load for job seekers. AI-driven features are highlighted with a signature sparkle motif (✦), signaling intelligence and premium value within a familiar, reliable structure.

## Colors

The color palette is engineered for professional trust and functional hierarchy. 
- **Primary Blue (#2563EB)** is the core driver for actions, branding, and primary AI signals.
- **Light Blue (#EFF6FF)** serves as a soft highlight for backgrounds of active states and AI-matched content.
- **Neutral Surface (#E8EAF0)** provides a distinct, cool-toned backdrop for white cards to sit upon, creating a natural sense of depth.
- **Typography** uses a high-contrast Slate-900 for primary readability and a softer Gray-500 for metadata and secondary descriptions.

## Typography

This design system utilizes **Inter** exclusively to maintain a utilitarian and highly legible interface. The scale relies on weighted hierarchy to guide the user through complex job descriptions and data-heavy dashboards.

Headlines use semi-bold weights with slight negative letter-spacing to appear tighter and more authoritative. Body text is optimized for long-form reading with generous line heights. Labels and small metadata are set in medium or semi-bold weights to ensure they remain legible against the light blue and white surfaces.

## Layout & Spacing

The system uses a **Fixed Grid** approach for desktop views to maintain focus and content density, centered within a 1280px container. 

- **Desktop (1024px+):** 12-column grid, 24px gutters, and 40px outer margins.
- **Tablet (768px - 1023px):** 8-column grid, 20px gutters, and 32px outer margins.
- **Mobile (Under 768px):** 4-column grid, 16px gutters, and 16px outer margins.

Spacing follows an 8px base unit. Components should prioritize vertical stacking with `stack-md` (16px) for related items and `stack-lg` (32px) for distinct sections.

## Elevation & Depth

Hierarchy is established through **Tonal Layering** and **Ambient Shadows**. 

The base layer is the page background (#E8EAF0). Interactive components like cards sit on the "Surface" level (#FFFFFF). To communicate interactivity, cards use a subtle, highly diffused shadow (y: 2, blur: 4, color: rgba(0,0,0,0.05)). 

On hover, cards transition to a "Raised" state, where the shadow deepens (y: 8, blur: 16, color: rgba(0,0,0,0.08)) and the element translates -4px on the Y-axis. Overlays and dropdowns use the highest elevation with a more pronounced shadow to separate them clearly from the content grid.

## Shapes

The shape language is consistently **Rounded**. 

The standard radius for cards and containers is 12px (0.75rem), providing a friendly yet professional silhouette. Buttons and status badges depart from this slightly by utilizing `rounded-full` (pill-shaped) geometry, which helps them stand out as actionable or informative "chips" against the rectangular grid of the job cards. 

Input fields and form elements should align with the 8px (0.5rem) standard to maintain a sharp, clean look.

## Components

### Buttons
- **Primary:** Rounded-full, #2563EB background, white text. Bold and high-contrast.
- **Secondary:** Rounded-full, outlined with #E5E7EB, #111827 text. Clean and understated.

### Cards
- White background, 12px radius, subtle shadow.
- Hover state: Lift up -4px with increased shadow depth.
- Inner padding: 24px for standard job cards.

### AI-Powered Features
- All AI suggestions, summaries, or matches must include the sparkle icon (✦) in #2563EB.
- Text associated with AI should often sit on a #EFF6FF background to differentiate it from static user data.

### Status & Badges
- **Match Badge:** #EFF6FF background, #2563EB text, rounded-full, used for "Match %" or "AI Fit."
- **Verified Badge:** A 16px blue circle with a white checkmark icon, placed immediately after company or user names.

### Inputs
- Background: #FFFFFF, Border: 1px #E5E7EB, Radius: 8px.
- Active/Focus: Border becomes #2563EB with a 2px soft blue outer glow.