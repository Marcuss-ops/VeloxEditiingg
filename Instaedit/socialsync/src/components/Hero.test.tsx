import { describe, expect, it } from "vitest";
import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { Hero } from "./Hero";

describe("Hero", () => {
  it("renders the main heading", () => {
    render(<Hero />);
    const heading = screen.getByRole("heading", { level: 1 });
    expect(heading.textContent).toContain("Log in");
    expect(heading.textContent).toContain("Post");
    expect(heading.textContent).toContain("everywhere");
  });

  it("renders the Inizia gratis link", () => {
    render(<Hero />);
    expect(screen.getByText("Inizia gratis")).toBeDefined();
  });

  it("renders the Guarda demo button", () => {
    render(<Hero />);
    expect(screen.getByTestId("watch-demo-button")).toBeDefined();
  });

  it("renders the platform names", () => {
    render(<Hero />);
    for (const name of ["Instagram", "Facebook", "TikTok", "YouTube", "X"]) {
      const matches = screen.getAllByText(name);
      expect(matches.length).toBeGreaterThan(0);
    }
  });

  it("opens the DemoModal when Guarda demo is clicked", async () => {
    render(<Hero />);
    const user = userEvent.setup();

    // Modal should be hidden initially.
    expect(screen.getByTestId("demo-modal-backdrop").classList.contains("hidden")).toBe(true);

    await user.click(screen.getByTestId("watch-demo-button"));

    // Modal should become visible.
    expect(screen.getByTestId("demo-modal-backdrop").classList.contains("flex")).toBe(true);
  });

  it("closes the DemoModal when the close button is clicked", async () => {
    render(<Hero />);
    const user = userEvent.setup();

    // Open the modal.
    await user.click(screen.getByTestId("watch-demo-button"));
    expect(screen.getByTestId("demo-modal-backdrop").classList.contains("flex")).toBe(true);

    // Close via the close button inside the modal.
    await user.click(screen.getByTestId("demo-modal-close"));
    expect(screen.getByTestId("demo-modal-backdrop").classList.contains("hidden")).toBe(true);
  });

  it("renders the upload mockup", () => {
    render(<Hero />);
    expect(screen.getByText("Nuovo post")).toBeDefined();
    expect(screen.getByText("estate-2026.mp4")).toBeDefined();
  });
});
