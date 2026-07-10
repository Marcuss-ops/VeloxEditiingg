import { describe, expect, it } from "vitest";
import { render, screen } from "@testing-library/react";
import { Footer } from "./Footer";

describe("Footer", () => {
  it("renders the copyright text", () => {
    render(<Footer />);
    expect(screen.getByText(/© 2026 SocialSync/)).toBeDefined();
  });

  it("renders the Privacy link", () => {
    render(<Footer />);
    const link = screen.getByText("Privacy");
    expect(link.closest("a")?.getAttribute("href")).toBe("#");
  });

  it("renders the Termini link", () => {
    render(<Footer />);
    const link = screen.getByText("Termini");
    expect(link.closest("a")?.getAttribute("href")).toBe("#");
  });

  it("renders the Contatti link", () => {
    render(<Footer />);
    const link = screen.getByText("Contatti");
    expect(link.closest("a")?.getAttribute("href")).toBe("#");
  });
});
