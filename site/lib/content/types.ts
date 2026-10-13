export type Block =
  | { type: "paragraph"; text: string }
  | { type: "heading"; level: 2 | 3; text: string }
  | { type: "code"; lang: string; code: string }
  | { type: "list"; items: string[] };

export type DocPage = {
  slug: string;
  title: string;
  description: string;
  blocks: Block[];
};

export type SiteContent = {
  home: {
    tagline: string;
    intro: string;
    features: { title: string; body: string }[];
    quickStart: { lang: string; code: string };
  };
  docs: DocPage[];
};