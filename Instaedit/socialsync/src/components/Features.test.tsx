import { describe, expect, it } from "vitest";
import { render, screen } from "@testing-library/react";
import { Features } from "./Features";

describe("Features", () => {
  it("renders the section heading", () => {
    render(<Features />);
    expect(
      screen.getByRole("heading", { name: /Tutto ciò che serve/i })
    ).toBeDefined();
  });

  it("renders the subtitle", () => {
    render(<Features />);
    expect(screen.getByText(/Design pulito/i)).toBeDefined();
  });

  it("renders all 4 feature cards", () => {
    render(<Features />);
    const titles = [
      "Calendario editoriale",
      "Adattamento automatico",
      "Analytics unificate",
      "Team e approvazioni",
    ];
    for (const title of titles) {
      expect(screen.getByText(title)).toBeDefined();
    }
  });

  it("renders each feature card with its description", () => {
    render(<Features />);
    expect(screen.getByText("trascina e pianifica")).toBeDefined();
    expect(screen.getByText("9:16, 1:1, 16:9 in un click")).toBeDefined();
    expect(screen.getByText("performance in un'unica dashboard")).toBeDefined();
    expect(screen.getByText("bozze, commenti, ruoli")).toBeDefined();
  });

  it("renders the security banner with Shield icon", () => {
    render(<Features />);
    expect(screen.getByText(/OAuth ufficiale/)).toBeDefined();
  });
});
