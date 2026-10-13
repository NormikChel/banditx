export const locales = ["ru", "en"] as const;
export type Locale = (typeof locales)[number];
export const defaultLocale: Locale = "ru";

export const ui = {
  ru: {
    brand: "banditx",
    navHome: "Главная",
    navDocs: "Документация",
    navGithub: "GitHub",
    themeLight: "Светлая",
    themeDark: "Тёмная",
    themeSystem: "Системная",
    language: "Язык",
    toc: "Содержание",
    prev: "Назад",
    next: "Далее",
    editOnGithub: "Редактировать на GitHub",
  },
  en: {
    brand: "banditx",
    navHome: "Home",
    navDocs: "Docs",
    navGithub: "GitHub",
    themeLight: "Light",
    themeDark: "Dark",
    themeSystem: "System",
    language: "Language",
    toc: "On this page",
    prev: "Previous",
    next: "Next",
    editOnGithub: "Edit on GitHub",
  },
} as const satisfies Record<Locale, Record<string, string>>;